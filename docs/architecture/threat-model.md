# Quoro threat model

Status: M0 draft for review.

This document is the threat model of the self-hosted Quoro product. It describes what is worth
protecting, who can attack it, where the trust boundaries are, and which threats the design already
answers. It is written against the contracts in `../contracts/` and the architecture in
[architecture.md](architecture.md); where the three disagree, an accepted decision wins over this
document, and this document wins over nothing.

## 1. Scope

In scope: the self-hosted product from M0 to M6 as defined in `../product/roadmap.md`. That is one
installation of the `quoro` binary in the roles `node`, `witness` and `edge` (D-0012), the HTTP API
and panel, the MCP server, the node-agent protocol, the engine on Cloud Hypervisor and KVM
(D-0007, D-0008), ingress and TLS (D-0024), the grant and approval model (D-0030), the audit log
(D-0031), webhooks and the AI-native loop (D-0032), the lease and fencing mechanism (D-0019), and
the later replication, witness and backup paths (D-0017, D-0023).

Out of scope:

- An optional managed offering may come later (D-0003); this document does not cover it.
- Live migration, distributed storage, SDN, multi-tenancy with billing and writing our own VMM
  (D-0034). None of them exists in M0-M6.
- Licensing and contributor agreements (D-0004). AGPL and the CLA are legal questions, not security
  controls, and are not discussed here.
- The security of third-party software as a product. Where this document makes a claim about
  external software that is not established in `docs/research/`, the claim is labelled
  **Hypothesis**.

## 2. Method

The model is built in five passes:

1. **Assets.** What has value to the operator or to an attacker, and where each asset lives.
2. **Actors.** Who can act on the system, and how far each actor is trusted.
3. **Trust boundaries.** Where data crosses from a less trusted to a more trusted region.
4. **STRIDE per component.** For each component, threats are classed as Spoofing, Tampering,
   Repudiation, Information disclosure, Denial of service or Elevation of privilege, and each row
   names the mitigation and what remains.
5. **Mitigations and residuals.** A map from decisions to the threats they answer, then the risks
   that stay open, then the functional checks that would show the mitigations work on the lab.

Every row in the STRIDE tables carries a reference: an accepted decision (D-xxxx), a research
finding (F-xxxx) or a research file, a recommendation marked pending maintainer decision, an open
question (Q-xxxx), a contract file under `../contracts/`, or the label **Hypothesis** / "design choice
of this document" when the statement rests on nothing else. A row without a reference would be an
assertion, and assertions are not accepted here.

Recommendations are pending maintainer decision. Where this document relies on one it says so, for
example "recommended, pending maintainer decision". Open questions stay open: this document lists the
options and never presents one as decided.

### 2.1 Assumptions

- **The host kernel and KVM are trusted.** Quoro runs on a host whose kernel the operator controls
  (D-0009). A kernel compromise is outside this model: the engine cannot defend against the layer it
  runs on.
- **Cloud Hypervisor is the VM isolation boundary** (D-0007, D-0008). One microVM per app, no Docker
  daemon on nodes (D-0008). Everything inside a guest is untrusted; the boundary is the hypervisor
  and the kernel beneath it.
- **An operator with root on the primary can do anything locally.** They can read the database,
  rewrite it, stop the process and edit the binary. This is not a flaw to be fixed; it is the reason
  the audit log has an off-site copy out of reach of whoever operates the server (D-0031) and why
  the chain only claims tamper *evidence*, not tamper prevention.
- **The network between nodes is hostile.** Nodes talk across the public internet, often from
  behind NAT (D-0010, D-0011). Every node connection is treated as if it were intercepted.
- **Agents are untrusted by default.** An agent may be prompt-injected by anything it reads: app
  logs, a webhook payload, an issue title. It acts only inside a grant (D-0029, D-0030).
- **The operator's browser is trusted until it is not.** A session cookie stolen from a compromised
  workstation is treated as the human, because the server has nothing else to go on.

## 3. Assets

| Asset | Where it lives | Why it matters |
| --- | --- | --- |
| Control-plane state and the change log | SQLite on the primary, replicated to replica nodes (D-0012, D-0013, `../contracts/db-schema.sql`) | It is the description of everything that runs. Changing it changes the infrastructure. |
| Audit log | `audit_log` table on the primary, chained (D-0031, `../contracts/audit-log-format.md`) | It is the only record of who did what. Destroying it destroys accountability. |
| Cluster signing key (Ed25519) | Primary, used to sign desired state | Whoever holds it can make any node run any workload. It is the root of node trust. |
| Node credentials | Hashed on the primary, root-only file on each node | A credential authorizes a node-agent session and nothing else, but that session carries desired state and app secrets for that node. |
| Human passwords, sessions and API tokens | Argon2id hashes and token hashes on the primary; cookie in the browser (D-0030, `../contracts/permission-model.md`) | A human identity carries a role bundle, which is most of the cluster. |
| Agent keys and access tokens | Agent keys hashed on the primary; access tokens short-lived (D-0029, D-0030) | A stolen access token impersonates the agent for up to five minutes, within whatever grants are active at each call; revoking the grant stops it on the next call (D-0030). |
| Grants and approvals | `grants`, `approvals` tables | They are the boundary between "an agent can ask" and "an agent can act". |
| App secrets (environment values) | `app_env`, encrypted at rest with the cluster key; decrypted per node for placed apps | They are database passwords and API keys of the operator's customers. |
| TLS private keys and the ACME account key | Node holding the `edge` or `node` role, root-only | A stolen key allows impersonation of the operator's domains. |
| Cloudflare and other provider API tokens | Provider configuration on the primary, encrypted at rest | They control DNS, load balancing and tunnels for every domain in the installation (D-0025). |
| Webhook secrets | `webhooks`, encrypted, write-only (D-0032) | They sign deliveries and prove that a webhook came from Quoro. |
| App data on volumes and in backups | Node disks, chunk store, S3-compatible bucket (D-0023) | It is the operator's actual data, and its loss is not recoverable from the control plane. |
| App logs and metrics | Node-local, queried on demand (D-0014) | They describe app behaviour and often contain secrets in URLs, headers or stack traces. |
| Release signing key | Release pipeline (D-0027) | A stolen key signs a malicious binary that the `curl \| sh` installer will run as root. |
| Availability of running apps | Nodes (D-0011) | The product promise is that apps keep serving when the panel does not. |
| The witness and the lease arbiter | The voters, a witness among them, in M5b (ADR 0169); an external arbiter is a later option (D-0017) | Whoever controls a majority of the voters, or the arbiter, decides who is primary. |

## 4. Actors

| Actor | Trust level | Notes |
| --- | --- | --- |
| Owner | Fully trusted human | Every scope, including `user:write` (`../contracts/README.md`). Can still be phished. |
| Operator | Trusted human | Every scope except `user:write`. Can enroll nodes (`node:enroll`) and decide approvals (`approval:decide`). |
| Viewer | Trusted human, read-mostly | Every `:read` scope plus `app:logs`. Cannot change anything, but can read app logs and audit records. |
| Agent | Untrusted by default | May be prompt-injected; acts only within a grant (D-0029, D-0030). Records who, on behalf of whom and why. |
| External agent runner woken by a webhook | Untrusted | Receives event payloads and incident context; its only authority is the incident grant (D-0032). |
| Node | Trusted for its own workloads only | May lie about itself, may be compromised. Cannot write control-plane state or read other nodes' secrets. |
| Primary | Trusted while it is the primary | Full control of state. A compromised primary is game over for confidentiality and integrity (see T-NODE-14). |
| Replica node | Trusted for its own workloads; holds a copy of the log | Sees control-plane state, including encrypted secrets. Cannot write (D-0012). |
| Witness | Trusted to vote, stores no data | Its only power is breaking ties (D-0017). A lying witness can stall an election but cannot read data. |
| Edge | Trusted, sees all ingress traffic | Terminates TLS, routes by hostname, can read plaintext HTTP (D-0018, D-0024). |
| Providers (Cloudflare DNS/LB/Tunnel, S3-compatible storage) | Trusted for availability, not for confidentiality of plaintext | They see traffic metadata, tunnel plaintext at the edge, and stored ciphertext (D-0025). |
| App workloads inside microVMs | Untrusted code | Arbitrary code supplied by the operator or by an image publisher. Must not reach the host or other apps. |
| Internet clients | Untrusted | Send arbitrary requests to app domains and to the API. |
| Upstream image publishers and the catalog | Trusted only through signatures and digests | The catalog pins images with verifiable signatures (D-0026). |
| Contributors and the release pipeline | Trusted through review and signing | A merged change ships to every installation (D-0004, D-0027). |
| A network attacker on the WAN between nodes | Hostile | Can drop, delay, replay and modify traffic (D-0011). |

## 5. Trust boundaries

```
        internet clients
              |
              v
   +------------------------+         boundary 1: unauthenticated traffic
   |  edge / ingress        |         terminates TLS, routes by hostname
   |  (proxy or Tunnel)     |
   +-----------+------------+
               |  boundary 2: app traffic into a guest
               v
   +------------------------+
   |  microVM (guest)       |  <-- untrusted code, one per app
   +-----------+------------+
               |  boundary 3: virtio devices, console socket, TAP
               v
   +------------------------+         boundary 4: host kernel / VMM
   |  node agent + engine   |         root on the node
   +-----------+------------+
               |  boundary 5: dial-out WebSocket over TLS, certificate pinned
               v
   +------------------------+
   |  primary (API, panel,  |
   |  MCP, audit, state)    |
   +------------------------+

   browser / panel  --boundary 6-->  API (session cookie)
   agent / MCP client --boundary 6--> API / MCP (bearer access token)
   primary --boundary 7--> webhook receiver (HMAC-signed HTTP POST)
   primary --boundary 8--> providers (Cloudflare, S3-compatible storage)
   node <--boundary 9--> node (mesh, from M5a)
   release pipeline --boundary 10--> operator host (curl | sh)
```

| # | Boundary | What crosses it | Control |
| --- | --- | --- | --- |
| 1 | internet -> edge | Arbitrary HTTP, TLS handshakes | TLS termination, hostname routing, per-app isolation (D-0024) |
| 2 | edge -> guest | HTTP requests for one app | Routing table derived from desired state, one microVM per app (D-0008) |
| 3 | guest <-> host | virtio-blk, virtio-net, virtio-console, serial console, TAP | Cloud Hypervisor device model, seccomp, one process per VM (`docs/research/cloud-hypervisor.md`) |
| 4 | node agent <-> engine | Engine trait calls, Cloud Hypervisor API socket | Local UNIX socket, node root only, `../contracts/engine-trait.md` |
| 5 | node -> primary | Node-agent session, desired state, events, leases | WebSocket over TLS with pinned certificate, Ed25519-signed desired state, `../contracts/node-agent-protocol.md` |
| 6 | human/agent -> API | Bearer token or session cookie, `Quoro-Justification` | Scope check, grant check, audit write in the same transaction (`../contracts/permission-model.md`) |
| 7 | primary -> webhook receiver | Event payload and `Quoro-Signature` | HMAC-SHA256 over `<t>.<raw body>`, 5 minute window (D-0032, `../contracts/README.md`) |
| 8 | Quoro -> providers | DNS records, LB pools, tunnel config, objects in a bucket | Provider API tokens scoped per provider; separate credentials per purpose (D-0025) |
| 9 | node <-> node | Replication traffic, chunk shipping (from M5a) | Mesh: kernel WireGuard over netlink, per-node keys and peers in desired state (D-0063), fencing tokens on every write (D-0019) |
| 10 | release pipeline -> host | A binary executed as root by the installer | Signature check plus reproducible build (D-0027) |

Boundary 5 is the most important one in the product: it is the only path by which instructions reach
a machine that runs workloads, and it crosses the public internet (D-0011). Boundaries 3 and 4 are
where untrusted code meets the host. Boundary 6 is where an untrusted agent meets authority.

## 6. STRIDE analysis

The tables below use threat ids `T-<AREA>-<nn>`. The STRIDE column holds one letter: S spoofing,
T tampering, R repudiation, I information disclosure, D denial of service, E elevation of privilege.

### (a) HTTP API and panel

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-API-01 | S | A stolen bearer token is replayed by an attacker | Tokens are stored only as hashes, carry an expiry, and a human token can be revoked; agent access tokens live 5 minutes | D-0030, `../contracts/permission-model.md` | Theft from a compromised workstation is indistinguishable from the human until revocation |
| T-API-02 | S | A stolen panel session cookie is used from another machine | Session cookie is an absolute 24 h session, stored hashed, bound to the user | `../contracts/permission-model.md` | No device binding; a copied cookie works until it expires |
| T-API-03 | T | A write sent to a replica node silently forks state | Only the primary writes; a write that reaches a replica that cannot forward answers `not_primary` (503) and never applies locally | D-0012, `../contracts/openapi.yaml` | A replica that wrongly believes it is primary would fork state; prevented by the promotion rules in (m) |
| T-API-04 | T | A `POST` is retried by a client or a proxy and creates a duplicate resource | `Idempotency-Key` is required on every creating `POST` | `../contracts/README.md` | Keys are scoped per identity and expire; a retry after expiry duplicates |
| T-API-05 | T | Cross-site request forgery against the panel's cookie-authenticated endpoints | Design choice of this document: state-changing panel requests require a non-simple header and the cookie is `SameSite=Lax`, so a cross-origin form post cannot carry them | design choice of this document | A same-site script injection still succeeds; see T-API-09 |
| T-API-06 | R | A human denies having made a change | Every mutating call by any identity is audited with actor, channel `panel` or `api`, params and result | D-0031, `../contracts/audit-log-format.md` | Human reads are deliberately not recorded, so "who looked at this" is unanswerable |
| T-API-07 | I | App environment values leak through a read endpoint | `app_env.value` is write-only: the API returns names only; `app:read` never returns a value | `../contracts/README.md` scope `app:read`, `../contracts/openapi.yaml` | A value still leaks through app logs if the app prints it (T-LOG-01) |
| T-API-08 | D | An unauthenticated client floods the API or an event stream | Authentication before work, per-identity rate limit answering `rate_limited` | `../contracts/permission-model.md` (rate limits are implementation defaults, not contract) | No global admission control in M0; a single identity can still consume the process |
| T-API-09 | E | A viewer escalates to owner through the panel | Human roles are fixed scope bundles; `user:write` is owner-only; no endpoint grants a role to oneself | `../contracts/README.md` roles, D-0030 | A viewer who obtains an owner's token is an owner (T-API-01) |
| T-API-10 | I | A `forbidden` response leaks which resources exist | Refusals use a fixed `reason` set and do not echo resource contents | `../contracts/README.md` forbidden reasons | `not_found` versus `forbidden` still reveals existence of ids the caller guessed |
| T-API-11 | S | Unclaimed first-run setup: between install and the first `POST /api/v1/setup`, anyone who reaches the listener can claim `owner` | The installer prints a one-time setup token, required in the `Quoro-Setup-Token` header; it is single use and expires after one hour, the primary stores only its hash, and `quoro setup-token` refuses once an owner exists (D-0054) | D-0054, architecture section 2, host layout | The token is printed on the host console and in the install log, so whoever reads that output within the hour can claim `owner` |

### (b) MCP server and agents

The MCP server is not a separate security path. Each tool maps one-to-one onto a service operation
and needs the same scope as the matching endpoint (D-0031, `../contracts/README.md`).

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-MCP-01 | E | Prompt injection in an app log, a webhook payload or an issue title makes the agent call a destructive tool | Destructive scopes are refused with `approval_required` unless a matching approval grant exists; the grant is bound to the target and params | D-0030, `../contracts/permission-model.md` | Injection can still spend the agent's non-destructive scopes, for example restart loops |
| T-MCP-02 | E | An agent is created or edited with an over-broad grant | `grant:write` and `agent:write` are human-only, so an agent can never widen itself; the wildcard `app:*` is not allowed in grants | `../contracts/README.md` scopes and flags, D-0030 | A careless human can create a broad grant; the panel must show the selector and TTL plainly |
| T-MCP-03 | S | An agent key is stolen from the runner's environment | The agent key authorizes nothing by itself; it only mints access tokens; disabling the agent invalidates tokens immediately; secrets are shown once and stored hashed | D-0029, `../contracts/permission-model.md` | Once minted, an access token is usable by whoever holds it for its lifetime |
| T-MCP-04 | S | An access token is replayed within its lifetime | 5 minute lifetime, opaque 32 random bytes, stored as SHA-256, minted per call sequence | `../contracts/permission-model.md` | No proof-of-possession; a token captured in transit or in a log is usable for up to 5 minutes |
| T-MCP-05 | E | An agent approves its own approval request | `approval:decide` is human-only, so the attempt is refused with `human_only` and audited | D-0030, `../contracts/README.md` | An agent can still social-engineer the human approver (T-GRT-06) |
| T-MCP-06 | E | A webhook-woken runner is used as a confused deputy to act outside the incident | The incident grant carries only the webhook's `wake.grant_scopes`, is limited to the incident subject, and is revoked when the incident resolves | D-0032, `../contracts/permission-model.md` | The subject selector is coarse: an incident on one app allows all listed scopes on that app |
| T-MCP-07 | R | An agent's stated justification is a plausible lie | The justification is recorded, not trusted: it is evidence for a human reader, and the decision to act was the grant's, not the text's | D-0031, header `Quoro-Justification` | Nothing verifies the text; a model can write a convincing wrong reason |
| T-MCP-08 | I | The MCP tools expose more data than the equivalent endpoint | Each tool calls the same service operation and needs the same scope; there is no MCP-only query | D-0031, `../contracts/README.md` MCP tools table | `get_app_logs` inherits the log risk (T-LOG-01) |
| T-MCP-09 | S | The MCP transport accepts a caller Quoro did not authenticate | Quoro validates the bearer access token itself before dispatching a tool; rmcp documents no inbound authorization middleware, so no such middleware is relied on (**Hypothesis**) | F-0041, `docs/research/rust-api-and-mcp-tooling.md` | If rmcp later adds server-side auth, two layers could disagree; the pinned version (F-0041) limits the surprise |
| T-MCP-10 | I | An agent reads app logs and extracts secrets to another system | `app:logs` is a scope and can be withheld; every agent call, including reads, is audited | D-0031, `../contracts/audit-log-format.md` | An agent with `app:logs` and network egress can exfiltrate what it reads; that is the grant owner's choice |
| T-MCP-11 | D | An agent in a restart loop harms availability within its scopes | Grants can carry `max_uses`; reads do not consume uses, mutating calls do | `../contracts/permission-model.md`, D-0030 | A grant with no `max_uses` and a long TTL permits unbounded restarts |

### (c) Grants and approvals

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-GRT-01 | E | Privilege escalation by creating a grant for oneself | `grant:write` is human-only; an agent holding it is refused with `human_only`; incident and approval grants are created by `system`, not by the agent | D-0030, `../contracts/README.md` scopes, `../contracts/permission-model.md` | Any human with `grant:write` can issue anything to any agent |
| T-GRT-02 | E | An approval grant is reused or widened to another target | Approval grants carry one scope, selector = that target only, `max_uses` 1 and a 10 minute TTL; the call must match `params_hash` | D-0030, `../contracts/permission-model.md` | A second identical destructive call needs a second approval; that is intentional friction |
| T-GRT-03 | E | A grant lives forever because its TTL was omitted | `expires_at` is mandatory on every grant; there is no non-expiring grant | D-0030, `../contracts/permission-model.md` | Expiry depends on the primary's clock; a clock jump shortens or lengthens a TTL (T-GRT-07) |
| T-GRT-04 | E | A revoked grant keeps working | Revocation is checked on every request in the authorization path, so the next call after revocation is refused with `grant_revoked` | D-0030, `../contracts/permission-model.md` | A call already executing when revocation lands completes; the audit record shows both |
| T-GRT-05 | E | An incident grant is widened beyond the incident | Incident grants may carry non-destructive scopes only, validated when the webhook is saved, and the selector is the incident subject only | D-0032, `../contracts/permission-model.md` | A webhook created with a broad `grant_scopes` list gives every matching incident that list |
| T-GRT-06 | S | Approval fatigue: a human approves a harmful request after many plausible ones | The panel must show scope, target, `params_hash` and justification together; the decision is audited with the deciding user | D-0030, `../contracts/audit-log-format.md` | Social engineering of the approver is not technically preventable |
| T-GRT-07 | T | Clock skew makes an expired grant usable | Expiry is evaluated against the primary's clock only; nodes do not evaluate grants | D-0030, `../contracts/permission-model.md` | A misconfigured or manipulated primary clock extends every TTL; the audit timestamp is affected too |
| T-GRT-08 | E | Time-of-check to time-of-use between approval and execution | The approval is bound to `target` and `params_hash`; a call with different parameters does not match the grant and is refused | D-0030, `../contracts/permission-model.md` | A matching call can still be harmful if the human misread the parameters |
| T-GRT-09 | E | An agent collects several narrow grants and combines them into a broad capability | Every request needs one matching grant for its scope and selector; uses are counted per grant; the audit names the grant used | `../contracts/permission-model.md`, D-0031 | Nothing prevents a human from issuing several grants to the same agent; combination is a human decision |
| T-GRT-10 | R | A refusal happens but is not recorded, so the attempt is invisible | Every refusal is audited with result `refused` and `result_detail` holding the forbidden reason | `../contracts/audit-log-format.md`, D-0031 | A refusal caused by a crash before the audit write is an `error` at best, or missing |

### (d) Audit log

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-AUD-01 | T | An operator edits one audit record to hide an action | Each record carries `hash` over its canonical JSON and `prev_hash` of its predecessor; verification reports `hash_mismatch` at that `seq` and emits `audit.chain_broken` | D-0031, `../contracts/audit-log-format.md` | The edit is evident, not prevented; if nobody verifies, nobody notices |
| T-AUD-02 | T | An operator truncates the tail of the log | Segments and head objects are copied off-site, so a truncated local tail disagrees with the last exported `last_seq` and `head_hash` | D-0031, `../contracts/audit-log-format.md` | Until an off-site copy exists there is none; the earliest is M5a (section 8.1), so truncation is undetectable before that |
| T-AUD-03 | T | An operator with root rewrites the whole log and its hashes | The off-site copy is written once with an object-lock retention in compliance mode; credentials are limited to put and cannot delete or overwrite a locked object | D-0031, `../contracts/audit-log-format.md` | Object-lock support is not portable across S3-compatible providers; the adapter probes it and must report "not locked" when the probe fails (F-0038) |
| T-AUD-04 | R | An agent claims a different actor made the change | Every record carries `actor_type`, `actor_id`, `agent_model`, `on_behalf_of`, `channel`, `request_id` and `grant_id` | D-0029, D-0031, `../contracts/audit-log-format.md` | The fields are filled by Quoro, not by the caller; a compromised primary can write anything (T-NODE-14) |
| T-AUD-05 | I | Secrets appear in the recorded `params` | A fixed redaction list (`value`, `values`, `secret`, `password`, `token`, `key` and every env value) replaces them with `[redacted]` | `../contracts/audit-log-format.md` | A secret in a key not on the list, for example a URL with a query token, is recorded verbatim |
| T-AUD-06 | T | An action exists without its audit record, or the reverse | The record is appended in the same SQLite transaction as the change it describes, and `change_seq` links it to the change-log entry | D-0031, D-0013, `../contracts/audit-log-format.md` | A crash between the transaction commit and the response is still one consistent state |
| T-AUD-07 | D | Audit write failure blocks all work, or work proceeds unaudited | The transaction couples them, so a failed audit write fails the action | `../contracts/audit-log-format.md` | A full disk on the primary stops writes; that is availability, not integrity (T-AVL-07) |
| T-AUD-08 | T | An attacker floods the log to bury an entry or to exhaust storage | Rate limits per identity; agents are rate limited like humans | `../contracts/permission-model.md` | No archival policy in M0-M6; growth is unbounded by design (retention would need its own decision) |
| T-AUD-09 | T | Deleting the off-site bucket credential's target objects | The audit-copy credential is put-only, separate from the backup credential, and locked objects cannot be overwritten | `../contracts/audit-log-format.md`, D-0025 | Bucket policy is provider configuration; a misconfigured bucket may accept deletes from another credential |
| T-AUD-10 | R | Verification itself is claimed but never run | `POST /api/v1/audit/verify` returns `{ok, checked, first_seq, last_seq, head_hash, broken_at_seq?, reason?}` and the verification run is audited | `../contracts/audit-log-format.md` | Nothing forces a schedule; an unverified chain is only as good as the next run |

### (e) Node-agent protocol

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-NODE-01 | S | A rogue machine enrolls as a node | Enrollment needs a one-time join token with a 1 h default expiry, created under `node:enroll`, which is human-only; the token is stored as a hash and marked used | D-0010, `../contracts/README.md` scope flags, `../contracts/node-agent-protocol.md` | A leaked unused token can be redeemed once within the hour |
| T-NODE-02 | S | A man-in-the-middle intercepts the node session | The join command carries the SHA-256 fingerprint of the primary's TLS certificate and the node pins it, so a self-signed certificate works and a substituted one does not | `../contracts/node-agent-protocol.md`, boundary 5 | Pinning survives only if the fingerprint was transmitted out of band, which is the operator's job |
| T-NODE-03 | S | An attacker impersonates an existing node | Each session needs the node credential, 32 random bytes, stored hashed on the primary and root-only on the node | `../contracts/node-agent-protocol.md` | A stolen node credential plus the pinned host allows impersonation; only one session per node exists at a time |
| T-NODE-04 | T | A forged `desired_state` makes a node run attacker workloads | Every `desired_state` is signed with the cluster Ed25519 key over the canonical JSON of its body; the node verifies before persisting | `../contracts/node-agent-protocol.md`, boundary 5 | The node trusts the cluster public key it received at enrollment; a wrong key at enrollment poisons everything after it |
| T-NODE-05 | T | Replay of an old, validly signed desired state | `rev` increases monotonically; the node rejects an older `rev` and acts on the newest verified stored state | `../contracts/node-agent-protocol.md`, D-0011 | Two states with the same `rev` cannot exist unless the primary is compromised |
| T-NODE-06 | T | Replay of an old log segment to roll state back | Each entry carries `prev_hash`/`hash`; the node verifies on receipt and on `hello`, and a divergence quarantines its copy instead of applying it | D-0013, D-0022, `../contracts/node-agent-protocol.md` | A node that never reconnects keeps a stale copy; that is expected, not a compromise |
| T-NODE-07 | I | A compromised node reads secrets of apps that are not placed on it | `env` values are decrypted per node and sent only for apps in that node's `desired_state` | `../contracts/node-agent-protocol.md` | A node can read everything for apps that *are* placed on it; that is unavoidable |
| T-NODE-08 | I | App secrets persist in plaintext on the node | The node stores `env` encrypted with a node-local key; the session carries them only over the pinned TLS connection | `../contracts/node-agent-protocol.md` | Root on the node can read the node-local key; see assumption 2.1 |
| T-NODE-09 | T | A compromised node lies about its status, health or events | Nothing can stop a node lying about itself | `../contracts/node-agent-protocol.md` | A lying node can trigger false `app.down`, false incidents and false agent wakes (T-WHK-05); control-plane state is not writable by a node |
| T-NODE-10 | T | A compromised node injects events to open incidents and wake agents | The primary assigns `seq` and dedups by `id`, records `source`, and the incident grant is limited to the subject with non-destructive scopes | D-0032, `../contracts/node-agent-protocol.md`, `../contracts/permission-model.md` | Noise and cost from false wakes; an incident on a node cannot grant destructive power |
| T-NODE-11 | D | A node is flooded with frames or oversized messages | One JSON message per text frame, 1 MiB maximum, heartbeat every 5 s and disconnect after 15 s without a frame | `../contracts/node-agent-protocol.md` | A slow-loris style session can hold the single session slot until it times out |
| T-NODE-12 | E | A node sends a message type or version the primary does not expect | Unknown fields are ignored, an unknown `type` is answered with a non-fatal `error`, and a major version mismatch is a fatal `unsupported_version` | `../contracts/node-agent-protocol.md` | A downgrade attack has nothing to gain: the protocol has one major version |
| T-NODE-13 | T | A removed node keeps receiving desired state | `DELETE /api/v1/nodes/{node_id}` ends the session with a fatal `node_removed` and the node stops dialing | `../contracts/node-agent-protocol.md` | Its VMs are left as they are; removal never destroys data remotely, by design |
| T-NODE-14 | E | A compromised primary takes over the cluster | Nothing in the protocol stops it: it holds the cluster signing key, the state and the audit log | D-0012, D-0031 | Full control. Signed desired state does not help, because the primary is the signer. This is why the audit copy must be out of reach of whoever operates the server (D-0031) and why a promoted machine obtains the keys only from seals made to its own custody key (ADR 0180, `../contracts/node-agent-protocol.md` section 7.2) |
| T-NODE-15 | S | An attacker who controls a node, or its network path, swaps the node's custody key for one it holds, so the next seals open for it | The primary binds each machine to the first custody key it reports and refuses a different one with `custody_key_changed`; it makes no seal to the new key and writes `node.custody_key_changed` to the audit log | ADR 0180, ADR 0231, `../contracts/node-agent-protocol.md` section 7.2 | The session is authenticated by the node credential, so an attacker who already holds the credential and the data directory also holds the original custody key and its seals (T-NODE-16) |
| T-NODE-16 | E | A compromised node with the `node` role opens its seals and acts as the primary | Accepted by D-0078: every promotable machine holds the keys a primary holds. A promotion through Quoro needs root on the machine and a majority of voters; a returning old primary demotes itself on the first higher term it sees | D-0078, ADR 0180, ADR 0182, `../contracts/node-agent-protocol.md` sections 7.3 and 7.4 | With the signing key and the primary's TLS key it can answer as the primary to any node that dials it, including through `welcome.primary_urls`, and claim a higher term: nodes check the certificate and the signature, not the votes. Its blast radius is that of a compromised primary (T-NODE-14) for the nodes that reach it |

### (f) Leases and fencing

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-LEASE-01 | T | Split brain: two nodes run the same stateful app and diverge | A stateful VM starts only while its node holds the app's lease in state `held` with the same `fencing_token` as the VM spec; every write to shared storage carries the token and receivers reject a lower one; from M5b a holder runs a lease that left `expiring` as `local` only once every machine with the `node` role holds the row's `local` write, so no later primary moves it on a gate (T-LEASE-16, ADR 0261) | D-0019, ADR 0261, `../contracts/node-agent-protocol.md`, `docs/research/r2-conditional-writes.md` | Fencing depends on every receiver checking the token; a new shared resource added later must implement it |
| T-LEASE-02 | T | An old lease holder keeps writing after losing the lease | The lease is expiring mode with a monotonic duration, and the node self-fences at `valid_until`; a failed forced stop never reports fenced (D-0074) | D-0019, `../contracts/node-agent-protocol.md` | A paused process can act between `valid_until` and the actual stop; the fencing token is what protects the data, not the timer |
| T-LEASE-03 | D | The arbiter is unreachable and nodes treat it as lease loss, stopping apps needlessly | "Arbiter unreachable" maps to `uncertain`: keep running, start nothing new stateful, and fence only at `valid_until`; local-mode leases never expire because the control plane is gone | D-0047, D-0011, `docs/research/r2-conditional-writes.md` | A long arbiter outage (a 59 minute R2 outage is recorded) still fences every expiring lease eventually |
| T-LEASE-04 | T | Clock drift between nodes makes a lease look valid after it expired | Durations are computed on a node clock that counts a host suspend (`CLOCK_BOOTTIME`), and the node brings every deadline earlier by a suspend it detects, at least every `M / 4` and at once on wake-up (ADR 0240), with `margin = max(1000 ms, duration_ms / 10)`; the authority only regrants after `duration_ms + 2 * margin`, and the primary measures its voters' support on the same clock, so a suspend of its host cannot stretch the window in which it renews (ADR 0263) | `../contracts/node-agent-protocol.md`; the drift bound itself is **Hypothesis** | The margin is a guess until measured on the lab; a large enough drift, or a machine paused from outside without its kernel counting the pause (a VM paused by its hypervisor), breaks the arithmetic |
| T-LEASE-05 | T | R2 answers 404 for a read while a conditional write is in flight, so a node believes it holds a lease it lost | Ambiguous outcomes are resolved with `If-None-Match: *` retries, and the fencing token, not the read result, decides what a shared resource accepts | `docs/research/r2-conditional-writes.md`, D-0019 | The negative-read window is a documented trap; the retry rule must be implemented, not assumed |
| T-LEASE-06 | T | A Durable Object restarts mid-write and comes back with pre-write state | An in-flight storage-touching request is terminated with an error, so "no definitive answer" means "lease not held"; the token still protects the data | `docs/research/durable-objects.md`, D-0017, Q-0012 (open) | Alarms are at-least-once, so expiry transitions must be idempotent; that is code-level care, not a protocol guarantee |
| T-LEASE-07 | D | Denial of service on the arbiter stops stateful apps | Accepted: an arbiter outage fences expiring leases, which is availability loss by design rather than corruption | D-0019, `docs/research/r2-conditional-writes.md` | A provider-wide outage becomes an app outage; the local lease mode limits the blast radius to `warm`/`hot` apps |
| T-LEASE-08 | T | A node with a valid credential claims a lease it never had | Lease authority is the primary in M0-M5a, and from M5b the primary while a majority of voters supports it (ADR 0169); a node cannot grant itself a lease | D-0017, D-0019, `../contracts/node-agent-protocol.md` | A compromised primary can grant a lease to any node (T-NODE-14) |
| T-LEASE-09 | T | A `local` lease never expires, so a lost node comes back and starts its stateful app beside the new holder | A local lease ends only by `lease_revoke` or when an operator declares the node lost, which increments the fencing token so the old holder's token is stale; a cold restore elsewhere needs the manual fence acknowledgement first (D-0047) | D-0047, `../contracts/node-agent-protocol.md` | The manual "declared lost" step is the weak link: forget it and two copies can start |
| T-LEASE-10 | D | Fencing takes longer than the margin and the node is still stopping when another starts | The node must finish fencing before `valid_until + margin`, and the authority waits `duration_ms + 2 * margin` before regranting; the engine keeps every start and stop within the bound its caller passes, and a stateful start under an `expiring` lease needs `valid_until - now >= start bound + M`, so a start never holds a VM past its fence (ADR 0242, ADR 0243) | `../contracts/node-agent-protocol.md` | A guest that ignores SIGTERM delays a force stop; the formula assumes the force stop works |
| T-LEASE-11 | T | A node that is not a warm volume's source, or a source that lost the lease, overwrites the warm copy a failover would start from | The target accepts a replication stream only from the source's mesh address named in its signed desired state, over the WireGuard peer that authenticates it; it refuses a fencing token below the highest it has seen and records the refusal; it writes into a staging file and keeps it only when every block hash and the root hash match, so its complete copy is never half-applied (ADR 0173) | D-0019, D-0173, `../contracts/node-agent-protocol.md` section 6.13 | The copy is only as good as its source: a compromised source that holds the lease can ship bad data with correct hashes |
| T-LEASE-12 | D | A slow fence of one lease holds back the renewal answer of another, which then fences although the primary renewed it | While a revoke's self-fence runs, the node still processes `lease_grant` for other leases, including renewal answers, liveness traffic and, on a voter, support requests, and holds back only the rest; the per-lease start gate keeps the revoked lease's VMs stopped (ADR 0222, ADR 0237) | D-0019, ADR 0222, ADR 0237, `../contracts/node-agent-protocol.md` sections 4.2 and 5.13 | Messages held back behind a long `local` fence, such as a new `desired_state` or another lease's revoke, wait until the revoke is answered |
| T-LEASE-14 | T | A lease converted from `local` to `expiring` runs with no deadline on the node while the primary already counts a gate for it, and a failover starts a second copy | The conversion grant counts toward no gate; the primary moves the lease only as a `local` one until a grant answers one of its renewals; the node renews at once and self-fences at the first renewal's provisional deadline, and before that renewal is sent behaves as `local` (ADR 0241); the lease row stays `local` until the primary answers that first renewal, so a primary that restarts before then gives the lease no gate, answers a failover with `lease_held`, and sends the conversion grant again when the holder's `hello` reports it `local` (ADR 0244); the first answer's gate counts the conversion grant's duration when that is longer, and the primary raises `leases.duration_ms` to it before the conversion, so a shorter answer lost in a partition cannot open the gate before the node fences (ADR 0254); an `expiring` grant that carries `re` never converts a lease, and a `local` grant drops the outstanding renewals, so a late answer cannot turn a lease back to `expiring` with no deadline (ADR 0256); after a same-token `local` grant the primary sends a conversion grant only once a `ping` sent after it is answered, so it never takes a renewal from before the conversion for the first one (ADR 0257), and after a session cut the holder's next `hello` stands in for that `pong`; the primary sends no conversion grant to a holder whose mode it has not learned on the current session, and when a holder's `hello` reports the lease `expiring` while its row reads `local`, the primary settles it: it answers a renewal and writes the row `expiring` only once the `pong` to a `ping` sent after that answer arrives, so a longer grant of an earlier primary cannot outlast a gate counted at `D` (ADR 0261) | ADR 0218, ADR 0241, ADR 0244, ADR 0254, ADR 0256, ADR 0257, ADR 0261, `../contracts/node-agent-protocol.md` sections 5.12 and 8.4 | Until the holder's first renewal is answered, a failover of the app needs the holder connected or declared lost, so a holder that dies right after the conversion costs an operator's declaration |
| T-LEASE-13 | T | An operator's pending acknowledgement of a failed fence clears a newer failed fence of the same lease that nobody saw, and the node starts stateful VMs while the old one may still run | A new failure of a lease takes it out of `fence_ack_pending` and lists it again in `fence_failure`, so it needs a new acknowledgement; the primary sends `fence_ack` only once the node has answered every `lease_revoke` of the session, so a failed fence of a revoke reaches the primary before the acknowledgement reaches the node (ADR 0238, ADR 0239); the `fence_ack` carries each lease's token after the acknowledgement, and the node clears only failed fences recorded below it, so a failure that crosses a `fence_ack` already on its way, a fence that fails while the command waits, or one whose `node.fence_failed` has not reached the primary yet, stays on the node and keeps it failed (ADR 0245); for a lease another node holds the token is one above that lease's, which clears a failure the node recorded at it, and after the acknowledgement the primary sends the node a revoke of that lease only at a session start, answered before the `fence_ack` goes out (ADR 0247); for a lease the primary has no row for the token is 2^53 - 1, the highest the change log carries, and the primary only revokes such a lease, at each session start (ADR 0262) | D-0079, ADR 0238, ADR 0239, ADR 0245, ADR 0247, `../contracts/node-agent-protocol.md` sections 5.17 and 8.5 | A node older than M5b ignores the tokens and clears every failed fence of a listed lease, and a primary older than M5b sends none: a mixed cluster keeps the gap until both sides are upgraded |
| T-LEASE-15 | T | A grant with a new token changes the mode of a lease the node holds: an `expiring` grant over a `local` lease leaves the node with no deadline while the primary counts a gate, and a `local` grant over an `expiring` one drops its deadline | A lease changes mode only through the same-token conversion; while the node holds the lease, the primary sends a new token only in the mode the node holds it in, and the node refuses a grant with a higher token and the other mode with `bad_message` (ADR 0258) | ADR 0258, `../contracts/node-agent-protocol.md` sections 5.12 and 8.4 | After `expired` or `revoked` a new token may carry either mode; the node then holds nothing to keep running |
| T-LEASE-16 | T | An app leaves `warm`, its holder takes the same-token `local` grant and runs the app with no deadline, and a successor primary that lacks the row's `local` write sees the lease `expiring` and, once its gate passes, starts the app on another node | The primary writes the row `local` first and sends the `local` grant only once every machine with the `node` role has acknowledged that write and while it holds majority support; until then the holder stays `expiring`, renews and fences only if the primary loses majority support. Every later primary holds the write: only sealed machines with the `node` role are promoted, acknowledgements are durable, a node's mark and the primary's `log_diverged` check refuse a log that went back, a refollowing node cannot be promoted, a later machine's seals come after the write, a removed machine leaves the required set only once its removal is replicated, and the candidate freezes its agent and builds from exactly the frozen log. A row that reads `local` while its holder may still be `expiring` moves only as a `local` lease does; the resend and the regrant from `hello` need majority support. A restore, or a promotion with `--accept-loss`, marks every `expiring` row unsettled, and such a row moves only as a `local` lease does until its holder reports; a restore is refused while another machine with the `node` role survives, unless the operator states otherwise with `--accept-loss` (ADR 0255, ADR 0261) | ADR 0255, ADR 0261, `../contracts/node-agent-protocol.md` sections 7.1, 7.3 and 8.4, `../contracts/backup-format.md` section 8.2 | A node host rolled back as a whole to a disk snapshot, or whose acknowledgements the primary lost in a crash, can still be promoted from a stale log; promoting from it needs `--accept-loss` by operator rule, which the command cannot enforce. The restore refusal can miss a node enrolled after the backup if the host's agent never learned of it. While a node is down or not following, the change off `warm` waits until it returns or is removed, and after any restore `warm` apps wait for their holders to report before they can move |

### (g) Engine and guest isolation

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-VM-01 | E | A guest escapes to the host through a virtio device or the VMM | One Cloud Hypervisor process per VM with a small device model (virtio-blk, virtio-net, virtio-console), a memory-safe VMM language, seccomp filters enabled by default and an optional Landlock sandbox | D-0007, D-0008, `docs/research/cloud-hypervisor.md` | A hypervisor or kernel bug is a host compromise; the device model is minimal but not empty. Sandboxing beyond seccomp/Landlock is **Hypothesis** |
| T-VM-02 | E | A guest attacks the host kernel through KVM | The kernel is trusted (assumption 2.1) and the guest runs unprivileged | D-0009 | Kernel vulnerabilities are out of scope; patch cadence is the operator's |
| T-VM-03 | D | One app starves another (noisy neighbour) | Each microVM gets its own vCPU and memory allocation from `resources`, and virtio I/O can be throttled with token buckets | `../contracts/engine-trait.md`, `docs/research/cloud-hypervisor.md` | CPU shares are not a hard cap on every host; a sustained load still affects neighbours |
| T-VM-04 | T | One app reaches another app's network | Each app has its own TAP on the node bridge and a private address; isolation between projects is enforced with nftables rules generated with the network | D-0008, `../contracts/engine-trait.md`, design choice of this document for the nftables default-deny | A misgenerated rule set can open a path; rules must be derived from desired state and tested |
| T-VM-05 | T | A malicious image compromises the node during preparation | Images are referenced with a digest and verified before conversion; the OCI-to-ext4 pipeline resolves layer paths inside the rootfs with `openat2` and `RESOLVE_IN_ROOT` (D-0061); the catalog pins images with verifiable signatures — **mitigation pending**: the catalog format and signature scheme are decided at M4 planning (D-0070) | D-0026, D-0061, `../contracts/engine-trait.md` | Conversion runs as root on the node; a parser bug in the image path is host code, which `RESOLVE_IN_ROOT` narrows but does not remove |
| T-VM-06 | I | A volume snapshot leaks guest secrets | A Cloud Hypervisor snapshot contains full guest RAM and no disk contents, so RAM must be treated as secret-bearing; the chunk engine encrypts chunks | `docs/research/cloud-hypervisor.md` (F-0035 finding), D-0023 | Snapshot contents include whatever the app had in memory, including keys |
| T-VM-07 | E | The guest's init is subverted to escape the microVM | The minimal init is PID 1 inside the guest and trusts nothing from the guest application; the app runs as PID 2 under the configured user | `../contracts/engine-trait.md`, D-0008 | The init is Quoro code inside a trusted boundary; a bug there is a guest-internal bug, not a host escape |
| T-VM-08 | D | A guest exhausts host disk with its rootfs or logs | Per-VM disk size from the VM spec, and `node.disk_low` is emitted when free disk falls below the node's threshold | `../contracts/README.md` event types, `../contracts/node-agent-protocol.md` | A full disk affects every app on that node before anyone reacts |
| T-VM-09 | E | Docker on the node gives a container a path to the host | There is no Docker daemon on nodes; apps are microVMs | D-0008, D-0034 | Builds in M4 run inside a builder microVM, not on the host (**Hypothesis** for the isolation of that builder until specified) |

### (h) Ingress and TLS

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-ING-01 | S | Someone attaches a hostname they do not control and obtains a certificate for it | A domain belongs to one app and is attached by a holder of `app:update`; ACME still requires control of the name to issue | D-0024, `../contracts/README.md` scopes | An operator can attach a hostname that ACME later validates through a DNS record they do control; the certificate then covers a name they own, which is not an attack |
| T-ING-02 | I | The ACME account key or a domain private key is stolen from the node | Keys are root-only on the node that terminates TLS | D-0024, assumption 2.1 | Root on that node reads them; that node is trusted |
| T-ING-03 | S | A stolen Cloudflare Tunnel token lets an attacker join the tunnel | The token is stored encrypted on the primary; a tunnel token authorizes a connector, not the API | D-0024, D-0025 | A rogue connector can receive and answer traffic for the tunnel's published hostnames |
| T-ING-04 | T | Health checks are spoofed so traffic goes to a dead node | Over a tunnel, an HTTPS monitor with `Host` = the app hostname reaches the app through the connector; the `HTTP_STATUS` route only proves connector liveness and must not be used as app health | F-0046, `docs/research/cloudflare-lb-and-tunnel.md`, D-0024 | Replicas of one tunnel UUID count as one endpoint, so a monitor can pass while another replica's local app is down |
| T-ING-05 | I | Plaintext leaves the app and crosses an untrusted network | TLS terminates at the edge or at Cloudflare; after that the hop to the guest is on the node bridge | D-0024 | Design choice of this document: the node-local hop is treated as trusted; if it is not, per-app TLS inside the guest is the operator's option |
| T-ING-06 | T | Host header confusion routes a request to the wrong app | Routing is by hostname from the verified route list in desired state, never by a client-supplied path or header beyond the hostname | `../contracts/node-agent-protocol.md` route shape, D-0024 | A shared hostname between apps cannot happen because a domain belongs to one app |
| T-ING-07 | D | A single edge is a traffic single point of failure | Documented; mitigations are Cloudflare as the traffic edge with the small host as witness only, or two edges with DNS failover | D-0018 | A self-hosted single edge loses all app traffic when it dies, while the apps keep running |
| T-ING-08 | T | An expired or wrong certificate serves traffic | `cert_status` is modelled (`pending`, `issued`, `renewing`, `failed`) and `certificate.expiring` warns before expiry | `../contracts/README.md`, D-0024 | Renewal failure is visible but the app is unreachable until it is fixed |
| T-ING-09 | S | Domain takeover after an app is deleted | Domains belong to an app and are removed with it; certificate material stays with the node until it is cleaned | `../contracts/README.md` relationships, D-0024 | A stale DNS record pointing at an old IP is the operator's to remove |

### (i) Providers and secrets

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-PRV-01 | E | A Cloudflare API token with account-wide scope is used to change unrelated DNS | Providers sit behind interfaces with per-provider credentials; the token should be scoped to the zone and to the products used | D-0025, design choice of this document for the least-privilege default | A user who pastes a global token gets global authority; Quoro cannot narrow it after the fact |
| T-PRV-02 | I | Backup credentials can also delete the audit copy | Backup and audit-copy credentials are separate: the audit-copy credential is its own put-only `object_store` provider entry (D-0051) | D-0051, `../contracts/audit-log-format.md`, D-0025 | The separation is a deployment property; both live on the same primary |
| T-PRV-03 | T | An S3-compatible provider silently ignores conditional writes, giving two nodes the same lease | The provider adapter probes the capability at setup and refuses to enable the arbiter if the probe fails | F-0038, `docs/research/r2-conditional-writes.md`, D-0017 | Community MinIO and Backblaze B2 status is **Hypothesis**; the probe is the only defence |
| T-PRV-04 | I | Stored backups are readable by the provider | Chunk-engine chunks are encrypted client-side; SQLite and template backups go through that same chunk engine from consistent snapshots, so nothing is stored unencrypted (D-0069) | D-0023, D-0069, F-0040 | Litestream with SSE-C may be added later as an optional backend (D-0069); such a backend would rely on the provider for encryption, with a Quoro-held key sent per request |
| T-PRV-05 | D | A provider rate limit (R2 allows 1 write per second per key) stalls the off-site copy | Segment keys are unique, so the per-key limit does not bind the audit copy; retries back off | F-0037, `../contracts/audit-log-format.md` | A provider-wide outage pauses the copy; the local chain continues |
| T-PRV-06 | I | Secrets in the change log are readable to a replica node | Replicated secret columns are encrypted with the cluster key, and a follower holds ciphertext. It holds the key only as a seal to its own custody key, which it opens when it is promoted | D-0013, ADR 0180, `../contracts/node-agent-protocol.md` sections 7.1 and 7.2 | Every machine with the `node` role can open its seal, so a compromised one reads every secret in the log (section 8.3, T-NODE-16) |
| T-PRV-07 | T | A provider changes a DNS record outside Quoro | Desired state is the source of truth and reconciliation rewrites drift | D-0011, D-0025 | Drift between reconciliations is unobserved unless a route check reports it |
| T-PRV-08 | I | Provider secrets read back through the API or the audit log | Provider secrets are write-only, encrypted with the data-encryption key in `secret_enc`, never returned by a read call, redacted from audit parameters, and reachable only under the human-only `provider:manage` scope | D-0051, D-0050 | A compromised primary holding the DEK can decrypt them |
| T-PRV-09 | I | The node credential hashes on every follower are used to open sessions | From M5b `node_credentials` travels in the change log so a promoted primary accepts every node's session; it holds only SHA-256 hashes of 32 random bytes, which cannot be reversed into a credential | ADR 0182, ADR 0226, `../contracts/db-schema.sql` migration 0012 | A follower learns which nodes have live credentials, which the `nodes` rows already show |

### (j) Webhooks

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-WHK-01 | S | A forged webhook delivery wakes an agent or triggers a receiver action | `Quoro-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<raw body>" with the webhook secret>`; receivers reject a signature older than 5 minutes | D-0032, `../contracts/README.md` request headers | The receiver must implement verification; Quoro cannot enforce it on a third party |
| T-WHK-02 | S | A captured delivery is replayed inside the 5 minute window | The timestamp window bounds it; deliveries also carry `Quoro-Event-Id` and `Quoro-Delivery-Id` for the receiver to dedupe | `../contracts/README.md` | A receiver that does not dedupe can be replayed within the window |
| T-WHK-03 | I | The webhook secret leaks | The secret is write-only, shown once, and stored encrypted | `../contracts/README.md` entity fields, D-0031 | A secret in a delivery log on the receiver is the receiver's problem |
| T-WHK-04 | E | Server-side request forgery: a webhook URL points at an internal address | URLs must be `https`, except `http://localhost` for testing; design choice of this document: private and link-local ranges are refused unless the operator opts in | `../contracts/README.md` entity fields, design choice of this document | DNS rebinding against an allowed host is not covered by a URL check |
| T-WHK-05 | T | A compromised node manufactures events that wake agents and spend money | Events are recorded with `source`, incidents are subject-scoped, and incident grants are non-destructive | D-0032, `../contracts/permission-model.md`, T-NODE-09 | False incidents produce false work; cost, not compromise |
| T-WHK-06 | D | Retry storms from a failing receiver amplify load | Exponential backoff from 10 s doubling to 1 h, at most 10 attempts, delivery records carry `status`, `response_code`, `error` | `../contracts/README.md` webhook delivery fields | A receiver that returns 500 quickly still costs 10 attempts per event |
| T-WHK-07 | E | One event wakes several runners that act on the same incident concurrently | Each gets its own incident grant with the same selector; the audit shows each call | D-0030, D-0032 | Two agents can interleave restarts; the grants do not serialize |

### (k) Telemetry

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-LOG-01 | I | App logs contain secrets and are read by a viewer or an agent | Logs stay on the node and are read on demand under `app:logs`; every agent read is audited | D-0014, D-0031, `../contracts/README.md` scope flags | `app:logs` is a broad read of everything the app printed, including tokens |
| T-LOG-02 | I | A caller reads logs of an app outside their selector | List results are filtered to matching resources, never refused wholesale | `../contracts/permission-model.md` | A selector with `{"all": true}` reads every log in the installation |
| T-LOG-03 | T | Log content injects markup or terminal escapes into the panel | Design choice of this document: log lines are rendered as text, never as HTML, and control characters are escaped in the terminal view | design choice of this document | A terminal emulator still interprets some sequences; the M4 terminal is scoped and audited |
| T-LOG-04 | D | A telemetry query is used to reach a node the caller cannot otherwise reach | The primary forwards the query over the existing node session and answers `node_unreachable` (504) when it cannot | `../contracts/README.md` error codes | The query path is the same session the control plane already has |
| T-LOG-05 | D | Log growth fills the node disk | Retention is local and the node emits `node.disk_low` | D-0014, `../contracts/README.md` event types | Detection is after the fact; a fast log flood can fill a disk between checks |
| T-LOG-06 | E | The M4 web terminal becomes a root shell with no trail | Terminal sessions require a scope, are recorded in the audit log, and run inside the app's guest | `../product/roadmap.md` M4 scope, D-0031 | The terminal gives whatever the guest gives; an agent with terminal access can act inside the app unlogged beyond the session record |

### (l) Supply chain and releases

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-SUP-01 | T | A tampered download is piped into a root shell | Binaries are signed and builds reproducible, so the installer can verify the signature before running the binary | D-0027 | The verification step is code in the installer; if it is skipped, the pipe still runs |
| T-SUP-02 | T | A build host is compromised and produces a malicious, correctly signed binary | Reproducible builds let anyone rebuild from source and compare; a mismatch is detectable | D-0027 | Detection needs someone to rebuild; the signature alone would not catch it |
| T-SUP-03 | S | The release signing key is stolen | It is a release-pipeline secret, never on the operator's host; releases are signed, so key custody is the control | D-0027 | A stolen key mints valid signatures; rotation and revocation are not defined in M0 |
| T-SUP-04 | T | An upstream catalog image is replaced with a different one | The catalog pins images by digest with verifiable signatures — **mitigation pending**: the catalog format and signature scheme are decided at M4 planning, before the first template ships (D-0070) | D-0026, D-0070 | A publisher that signs a malicious image passes every check; the signature proves origin, not intent |
| T-SUP-05 | T | A dependency upgrade breaks a security invariant (for example MCP auth) | rmcp is pinned to a minor version and majors are budgeted; the API spec is frozen and diffed in CI | F-0041, F-0042, D-0048, D-0036 | A pinned old version accumulates unpatched issues; the upgrade budget is a process, not a guarantee |
| T-SUP-06 | E | A contributor merges a backdoor | Review before merge, one maintainer merges, and contracts are standalone pull requests | D-0004, D-0036 | Review is human; a subtle change can pass |
| T-SUP-07 | T | The panel client is generated from a hand-edited spec that diverged from the server | The hand-written `../contracts/openapi.yaml` is frozen and CI fails on a semantic diff against the utoipa output; orval generates the panel client from it | D-0048, F-0042 | CI compares documents, not behaviour; a handler can still differ from its annotation |

### (m) Availability

| Id | S | Threat | Mitigation | Reference | Residual risk |
| --- | --- | --- | --- | --- | --- |
| T-AVL-01 | D | The control plane dies and apps stop | Nodes store desired state locally and reconcile without the control plane, and they keep self-restarting apps | D-0011, D-0010 | Changes are blocked until a primary exists again |
| T-AVL-02 | D | The primary is lost and nobody can change anything | From M5b an operator promotes a machine with the `node` role with `quoro cluster promote`: it rebuilds the database from its followed log, opens its seals and serves with the old certificate, so nodes follow it without a re-pin | D-0012, D-0016, ADR 0182, `../contracts/node-agent-protocol.md` section 7.3 | Promotion is an operator action; until it happens changes are blocked. Entries the old primary wrote and the candidate never followed are lost, which can only be entries some machine with the `node` role never acknowledged unless the operator promotes with `--accept-loss` (ADR 0261), and the new primary rebuilds browser sessions, webhook delivery state and the vulnerability mirror from nothing |
| T-AVL-03 | D | Two nodes both believe they are primary after a network cut | Automatic election is not allowed below three votes, and Quoro says so instead of pretending. A promotion takes a higher term from a majority of voters and waits for the old primary's grants; every machine refuses a lower term, and an old primary that meets the higher term stops writing (ADR 0182). The candidate takes its term above every term it has seen or a vote answer carries, and freezes its own agent before the real vote so the log it builds from cannot change. A restored primary starts above every term it knows of, and a restore is refused while another machine with the `node` role survives (ADR 0261) | D-0016, D-0017, ADR 0169, ADR 0230, ADR 0236, ADR 0261, `../contracts/node-agent-protocol.md` sections 7.3, 7.4 and 8.9, `../contracts/backup-format.md` section 8.2 | `--ack-fence` skips the wait on the operator's word: if the old primary still runs, an `expiring` lease it granted in its last seconds may run twice. `--accept-loss`, for a promotion or a restore, gives up replicated entries on the operator's word, and a token issued in lost entries may be issued again to another node; a restore with it is safe only if, as the operator states, no other machine with the `node` role survives and no control plane of the cluster runs. An old primary cut off with some nodes keeps changing their desired state and `local` leases until they see the new term |
| T-AVL-04 | D | The witness sits in the same failure domain as a node and votes with it | The requirement is stated in the product and the panel; the witness must be outside the nodes' failure domain | D-0017 | Nothing enforces placement; a home PC next to a node is accepted if the operator says so |
| T-AVL-05 | D | A single edge loses all app traffic | Two edges with DNS failover, or Cloudflare as the traffic edge with the host as witness only | D-0018 | Both alternatives add a provider dependency |
| T-AVL-06 | D | A provider outage removes the arbiter and stops stateful apps | The outage fences expiring leases by design; local-mode leases keep `none`/`cold` apps running | D-0047, D-0019, `docs/research/r2-conditional-writes.md` | `warm` and `hot` apps lose availability during a long outage |
| T-AVL-07 | D | The primary's disk fills with change log and audit records | `node.disk_low` exists for nodes; the primary is a node too | D-0013, D-0031, `../contracts/README.md` event types | No archival policy in M0-M6; growth is bounded only by the operator |
| T-AVL-08 | D | A crash-heavy app consumes restart budget and hides a real fault | Restart backoff from 1 s doubling to 5 min, reset after 10 minutes healthy; a failed start, a `Timeout` included, waits the same backoff before the next try (ADR 0250); `app.down` after the failure threshold | ADR 0250, `../contracts/node-agent-protocol.md` section 6.3, `../contracts/README.md` event types | A crash loop is reported, not fixed; it still burns CPU on the node |
| T-AVL-09 | D | Upgrading the binary stops apps | The node agent restart does not restart VMs, because VMMs are separate processes and are rediscovered | D-0011, ADR 0224, `../contracts/engine-trait.md`, `../contracts/node-agent-protocol.md` section 6.3 | A VMM crash during the upgrade window is not covered by the agent restart. An agent that restarts without a verified desired-state snapshot and then receives a revoke for a lease with no recorded fence stops every app VM on its node until the first desired state verifies (ADR 0224): a short outage chosen over the risk of two copies of one app |

## 7. Mitigation map

The map is the reverse view: which threats each accepted decision answers. It is not exhaustive for
decisions with a single narrow threat.

| Decision | Threats it mitigates |
| --- | --- |
| D-0011 (autonomous nodes, dial-out) | T-AVL-01, T-NODE-05, T-API-03 (no inbound path to a node to attack), T-AVL-09 |
| D-0013 (numbered hash-chained log) | T-NODE-06, T-PRV-06, T-AUD-06, T-AVL-02 (a replica can verify what it holds), T-AUD-01 (the same chain discipline) |
| D-0017 (three votes, witness) | T-AVL-03, T-AVL-04, T-LEASE-08, T-PRV-03 (the arbiter must be probed before it votes) |
| D-0019 (mandatory lease-based fencing) | T-LEASE-01, T-LEASE-02, T-LEASE-07, T-LEASE-10, T-LEASE-11, T-LEASE-05 (tokens do the work, not reads), T-VM-06 (shared storage writes carry tokens) |
| D-0026 (curated catalog, pinned signed images) | T-VM-05, T-SUP-04 |
| D-0027 (signed binaries, reproducible builds) | T-SUP-01, T-SUP-02, T-SUP-03 |
| D-0029 (agents as separate identities) | T-MCP-03, T-AUD-04, T-MCP-07, T-API-06 |
| D-0030 (scoped, time-limited grants, JIT approval) | T-MCP-01, T-MCP-02, T-MCP-04, T-MCP-05, T-MCP-11, T-GRT-01 to T-GRT-09, T-API-09 |
| D-0031 (append-only audit, same path for MCP) | T-AUD-01 to T-AUD-10, T-MCP-07, T-MCP-08, T-MCP-10, T-API-06, T-NODE-10, T-GRT-10, T-SUP-07 (the frozen spec and its CI diff support it, D-0048) |
| D-0032 (detect, notify, wake, act) | T-WHK-01 to T-WHK-07, T-MCP-06, T-NODE-10, T-GRT-05 |
| D-0012 (one primary, replicas) | T-API-03, T-AVL-02, T-NODE-14 (it names the trust concentration instead of hiding it) |
| D-0014 (telemetry stays on nodes) | T-LOG-01 (blast radius), T-LOG-02, T-PRV-04 (logs are not copied to a provider) |
| D-0018 (edge as third voter) | T-AVL-05, T-AVL-03 |
| D-0024 (ingress with ACME or Tunnel) | T-ING-01 to T-ING-09 except availability-only rows |
| D-0025 (provider interfaces) | T-PRV-01, T-PRV-02, T-PRV-07, T-PRV-03 (a probe per provider is possible only with an interface) |
| D-0007/D-0008 (own engine, microVM per app, no Docker) | T-VM-01 to T-VM-09 |
| D-0010 (independent nodes) | T-AVL-03, T-API-03 |
| D-0047 (lease modes, fencing tokens on every shared write) | T-LEASE-02, T-LEASE-03, T-LEASE-05, T-LEASE-09, T-AVL-06, T-VM-06 |
| D-0048 (frozen spec, CI diff, pinned rmcp) | T-SUP-05, T-SUP-07 |
| D-0050 (cluster keys, DEK storage and rotation) | T-PRV-06, T-PRV-08 |
| ADR 0180 to ADR 0182 (key custody, the kept certificate, manual promotion) | T-AVL-02, T-AVL-03, T-PRV-06, T-NODE-15 |
| ADR 0255 and ADR 0261 (every node holds a lease row's `local` write before an `expiring` lease's renewals end) | T-LEASE-16, T-LEASE-14, T-LEASE-01, T-AVL-02, T-AVL-03 |
| D-0051 (human-only `providers` resource, write-only secrets) | T-PRV-01, T-PRV-02, T-PRV-08 |
| D-0053 (off-site copy in M5a, object-lock probe) | T-AUD-06, T-PRV-05 |
| D-0054 (setup token, listener and pin model) | T-API-11 |
| D-0059 (embedded reverse proxy behind the `Ingress` interface) | T-ING-01 to T-ING-09 (the proxy and its certificate handling are Quoro code; Caddy remains a fallback backend) |
| D-0061 (own init, own OCI-to-ext4 pipeline with `RESOLVE_IN_ROOT`) | T-VM-05 (layer paths cannot escape the rootfs), T-VM-07 (PID 1 is Quoro's own init) |
| D-0063 (kernel WireGuard configured over netlink) | Boundary 9, node <-> node: replication traffic and chunks cross it under per-node keys instead of trusting the network |
| D-0069 (own chunk engine for SQLite and template backups) | T-PRV-04, T-AUD-06 (consistent snapshots make the copy worth restoring) |

## 8. Residual risks and open items

### 8.1 A root operator on the primary before the off-site audit copy exists

D-0031 asks for an off-site copy with object lock. The copy ships in M5a with the S3-compatible path, gated by an object-lock probe per provider (D-0053). Until the first deployment of that copy, an operator with root on the primary can rewrite or truncate the log and nothing outside the machine contradicts them. The hash chain still catches a careless edit, because verification reports the exact `seq`. It does not catch a careful rewrite of the tail or of the whole file.

This is the single largest gap in the model. It is a scheduling fact, not a design flaw, and it
should be stated as such in the M3 demo: "the log is tamper-evident, not tamper-proof, until the
off-site copy exists".

### 8.2 Single primary until promotion exists

From M5b an operator can promote a machine with the `node` role when the primary is gone
(D-0067, ADR 0182, `../contracts/node-agent-protocol.md` section 7.3); until M6 there is no
automatic election. One primary therefore stays a single point of write failure until an operator
acts, and a single point of trust: compromising it means full control (T-NODE-14). Following
machines reduce the data-loss risk, not the authority risk, and each of them holds the keys a
primary holds (section 8.3).

### 8.3 Cluster key custody and how a promoted replica obtains it

Replicated secret columns are encrypted with the cluster key (D-0013, `../contracts/node-agent-protocol.md`).
A promoted replica must obtain that key or it cannot read the state it now owns. The algorithms, the
storage and the rotation of the cluster keys are decided (D-0050; architecture section 6, "Cluster
keys"), and so is custody on promotion (D-0078, read by ADR 0180 and ADR 0181): every machine with
the `node` role generates a custody key pair, and the primary seals the signing key, the
data-encryption key and its own TLS key to each such machine. Edge-only and witness-only machines get
no seal. This widens the blast radius of a compromised node: a node that can open its seals holds
the keys a primary holds, which D-0078 accepted (T-NODE-16). The seals are HPKE seals to an X25519
custody key that never leaves the machine, bound to one cluster, machine, kind and key, and the
primary refuses a machine's attempt to change its custody key (T-NODE-15;
`../contracts/node-agent-protocol.md` section 7.2). That is why promotion is not offered before
M5b: a replica promoted earlier would own state it cannot decrypt.

### 8.4 Object lock portability

The off-site audit copy depends on object-lock support in compliance mode, which is not portable
across S3-compatible providers (F-0038). The contract requires the adapter to probe and to refuse to
report "locked" when the probe fails (`../contracts/audit-log-format.md`). A provider that passes the
probe but degrades later is not covered. Object-lock support per provider is **Hypothesis**: it is not
in `docs/research/`.

### 8.5 Database backups and encryption

Litestream 0.5.x has no client-side encryption (F-0040), so a SQLite backup taken through it would
rely on provider-side SSE-C or SSE-KMS. D-0069 settles the default instead: SQLite and catalog-template
backups use Quoro's own chunk engine with client-side encryption, from consistent snapshots taken with
the SQLite backup API or `VACUUM INTO`. Litestream is not used. A Litestream backend with SSE-C may be
added later as an optional path for a lower RPO; it would then rely on the provider for encryption,
with a Quoro-held key sent per request, and pinning the older 0.3.x line with age stays a
**Hypothesis** on its maintenance state.

### 8.6 Hypotheses carried by this document

| Hypothesis | Where it matters | What would settle it |
| --- | --- | --- |
| Monotonic clock drift between machines is bounded well below `max(1000 ms, duration_ms / 10)` | T-LEASE-04, lease timing | A lab measurement of drift under load and after suspend |
| Cloud Hypervisor's device model plus seccomp and Landlock make guest escape a hypervisor-bug problem, not a configuration problem | T-VM-01 | A lab escape test and a review of the per-VM sandbox flags |
| rmcp does not validate inbound tokens for a server, so Quoro must | T-MCP-09 | Reading the pinned rmcp version's server code |
| A provider passes the object-lock probe and keeps honouring it | T-AUD-03, 8.4 | A per-provider test that writes, locks and tries to delete |
| The M4 builder microVM is isolated from the host the same way an app microVM is | T-VM-09 | The M4 build brief and a test that the builder cannot reach the node filesystem |
| Cloudflare LB failover over a tunnel is app-level only with the `Host` monitor | T-ING-04 | F-0046 already narrows it; a lab test would confirm the replica caveat |
| No Docker on nodes means no container escape path | T-VM-09 | Trivially true by construction; re-check if a container engine is ever added |

### 8.7 Items this document deliberately does not decide

- Retention of audit records and of app logs. Both are "never deleted" in M0-M6; a policy needs its
  own decision.
- Key rotation for the release signing key and node credentials. The signing-key and data-encryption-key
  rotation of the cluster keys is defined (D-0050; architecture section 6, "Cluster keys"); the release
  key's rotation is not defined.
- Federation or multi-installation trust. Out of scope (D-0034).
- The threat model of the managed offering.

## 9. Security testing on the lab

Functional checks only. The lab is nested virtualization and is not valid for performance work
(`../product/roadmap.md`). Each item below is a check a person can run over SSH on the lab hosts, and
each names the threat it exercises.

### M0 — contracts

| Check | Threat |
| --- | --- |
| Take the `desired_state` message from the protocol contract, change one field, keep the old signature, and confirm the verification step rejects it | T-NODE-04 |
| Send a `desired_state` with `rev` lower than the stored one and confirm it is rejected | T-NODE-05 |
| Revoke a local lease and confirm the node runs the self-fence path and emits `node.fenced` | T-LEASE-01 |
| Expire a test-only short expiring lease and confirm the node fences before `valid_until + margin` | T-LEASE-10, T-LEASE-04 |
| Make a forced stop fail during a fence and confirm the node emits `node.fence_failed`, not `node.fenced`, and starts no stateful VM | T-LEASE-01 |

### M1 — installer and engine

| Check | Threat |
| --- | --- |
| Modify one byte of the published binary and confirm the installer refuses to run it | T-SUP-01 |
| Rebuild from source and compare the artifact hash with the published one | T-SUP-02 |
| From inside a guest, attempt to read a host file path and to reach another VM's address; both must fail | T-VM-01, T-VM-04 |
| Fill a guest disk and confirm `node.disk_low` fires for the node | T-VM-08 |

### M2 — node agent and ingress

| Check | Threat |
| --- | --- |
| Present a revoked or unknown node credential to the session endpoint and confirm the connection is refused | T-NODE-03 |
| Reuse a join token twice and confirm the second enrollment fails | T-NODE-01 |
| Connect with a mismatched TLS certificate and confirm pinning rejects the session | T-NODE-02 |
| Change a hostname in the panel to a domain the operator does not control and confirm ACME fails rather than issuing | T-ING-01 |
| Delete an app and confirm its route stops resolving to the old guest | T-ING-06 |

### M3 — agents, grants, audit

| Check | Threat |
| --- | --- |
| An agent with a read-only grant attempts a restart: refused, reason `scope_missing`, refusal audited | T-GRT-01, T-GRT-10 |
| An agent attempts a delete without approval: `approval_required`, no deletion, approval request pending | T-MCP-01, T-GRT-02 |
| Approve, then call with changed parameters: refused because `params_hash` does not match | T-GRT-08 |
| Reuse the approval grant: the second call is refused with `grant_exhausted` | T-GRT-02 |
| An agent attempts `approval:decide` or `grant:write`: refused with `human_only` and audited | T-MCP-05, T-GRT-01 |
| Revoke a grant and confirm the next call is refused with `grant_revoked` | T-GRT-04 |
| Edit a copy of the audit log and confirm `audit.verify` reports the exact `seq` and `audit.chain_broken` fires | T-AUD-01, T-AUD-10 |
| Send a webhook delivery with a stale timestamp and with a wrong HMAC; the receiver rejects both | T-WHK-01, T-WHK-02 |
| Create a webhook with `http://` to a private address and confirm it is refused | T-WHK-04 |
| Read an app's env values through the API and confirm only names come back | T-API-07 |
| Inspect an audit record for a call that carried a secret in `params` and confirm `[redacted]` | T-AUD-05 |

### M4 — builds, terminal, console

| Check | Threat |
| --- | --- |
| Run the web terminal and confirm the session appears in the audit log with the acting identity | T-LOG-06 |
| Print a line with terminal escape sequences from an app and confirm the panel renders it as text | T-LOG-03 |
| Run a build and confirm the builder cannot read the node's credential file | T-VM-09 |

### M5a — replicas, edge, backups

| Check | Threat |
| --- | --- |
| Run the object-lock probe against the configured bucket and confirm the panel reports locked or not-locked truthfully | T-AUD-03, T-PRV-03 |
| Attempt to delete a locked audit segment with the audit-copy credential: refused | T-AUD-09, T-PRV-02 |
| Read a backup chunk from the bucket and confirm it is ciphertext | T-PRV-04 |
| Restore a primary snapshot taken through the SQLite backup API and confirm the off-site copy is ciphertext, encrypted client-side with the cluster key | T-PRV-04 |

### M5b — witness, leases, fencing

Besides the lab run, each of these checks also runs as an automated test in the M5b work. The cut on the lab is a firewall rule on lab-1, removed by a timer; lab-1's agent keeps running. The arbiter row applies only when an external arbiter is used; with the witness instead, the same check makes the primary lose the support of a majority of voters, and the node reports `uncertain` and does not fence before `valid_until` (D-0169).

| Check | Threat |
| --- | --- |
| Cut lab-1 off from lab-2 and lab-3: lab-1 self-fences its stateful apps and they stop | T-LEASE-02 |
| While lab-1 holds the lease, ask to start the warm copy on lab-2: refused | T-LEASE-01, T-LEASE-08 |
| Reconnect lab-1: it starts nothing stateful, its copy becomes the warm copy, and the quarantined snapshot is in the audit log | T-NODE-06, T-LEASE-09 |
| Make the arbiter unreachable, if an external arbiter is used, and confirm the node reports `uncertain` and does not fence before `valid_until` | T-LEASE-03, T-LEASE-07 |
| Present a stale fencing token to the chunk store and confirm the write is rejected | T-LEASE-01, T-LEASE-05 |
| Let several `expiring` leases on one node expire at the same time and confirm every fence runs concurrently and finishes within its own bound, `t_send + D` (D-0075) | T-LEASE-01, T-LEASE-02 |
| Crash a node in the middle of a fence and confirm that, after restart, its stateful apps stay stopped and no lease is granted elsewhere until an operator acknowledges the fence (D-0075) | T-LEASE-02, T-LEASE-09 |
| Ask for manual promotion while the old primary may still hold expiring leases: refused until an operator acknowledges the fence (D-0067) | T-AVL-02, T-AVL-03 |
| Promote while the old primary runs and its voters support it: every vote is refused (`support_held`) and no voter moves to the new term | T-AVL-03 |
| Stop the old primary and promote another machine: the nodes and the edge reach it with their pins unchanged, and it reads an environment value of an app, so its seal opened | T-AVL-02, T-PRV-06 |
| Start the old primary again after the promotion: on the first `stale_term` it ends its sessions with `not_primary` and answers `503`; `quoro node rejoin` keeps its data as a quarantined copy | T-AVL-03 |
| Report a different custody key from an enrolled node: refused with `custody_key_changed`, no seal is made to it, and the audit log records it | T-NODE-15 |

### M6 — election and hot

| Check | Threat |
| --- | --- |
| Cut the network into a minority partition and confirm no node in it accepts writes or claims to be primary | T-AVL-03 |
| Power off the primary and confirm a new primary takes writes with no human action | T-AVL-02 |
| After the partition heals, confirm writes succeeded in the majority and the audit chain still verifies | T-AUD-01, T-AVL-03 |

## 10. References

- Decisions and findings: the decision ledger, mirrored as ADRs in `../adr/`.
- Contracts: `../contracts/README.md`, `../contracts/openapi.yaml`, `../contracts/events.schema.json`,
  `../contracts/node-agent-protocol.md`, `../contracts/audit-log-format.md`,
  `../contracts/permission-model.md`, `../contracts/db-schema.sql`, `../contracts/engine-trait.md`.
- Architecture and product: [architecture.md](architecture.md), `../product/prd.md`,
  `../product/roadmap.md`, `../glossary.md`.
- Research: `docs/research/cloud-hypervisor.md`, `docs/research/r2-conditional-writes.md`,
  `docs/research/durable-objects.md`, `docs/research/cloudflare-lb-and-tunnel.md`,
  `docs/research/rust-api-and-mcp-tooling.md`, `docs/research/vps-nested-virt.md`.
