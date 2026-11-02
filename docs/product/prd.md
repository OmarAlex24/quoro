# Quoro: Product Requirements (v2)

Status: draft for review. Source of truth: the decision ledger; where this document and an accepted decision disagree, the decision wins.

## 1. Summary

Quoro is one self-hosted tool that unifies a hypervisor with application deployment [D-0002]. One engine, built on Cloud Hypervisor and KVM, runs one microVM per app, and the same interface deploys, exposes and observes those apps: push-to-deploy, domains, TLS, logs [D-0002, D-0008]. The agent is not an add-on: agents are first-class operators through MCP and events from day one, reading the same state as the panel, acting through the same API and being logged the same way [D-0002].

Quoro starts as a self-use and experimenter tool: anyone can install it on a box they have and experiment [D-0003]. Enterprise and managed offerings come later [D-0003].

The core is AGPL-3.0. Enterprise code, when it exists, lives in `ee/` under a separate license, and a CLA is required from the first external contributor [D-0004].

## 2. Problem

A self-hoster who wants both virtual machines and application deploys today stacks two tools: Proxmox for the hypervisor, then a Debian VM inside it running Dokploy or Coolify for the apps [D-0002]. The result is two dashboards, two mental models and two upgrade paths, and neither of them was designed with agents in mind [D-0002].

The tools that do cover both layers are Kubernetes-heavy and aimed at hosting providers rather than at one person with a few machines: Harvester with Rancher, and Cozystack [D-0002, F-0006]. They solve a different problem at a different cost.

What is missing is a single tool that owns the VM layer and the deploy layer, keeps the operational model small, and treats an AI agent as an ordinary, auditable operator rather than as an external script with root [D-0002, D-0029, D-0031].

## 3. Users and personas

Primary user now: the self-use / experimenter. Someone who has a machine or a VPS, installs Quoro, and tries to run a real app on it [D-0003].

Three personas follow from that [D-0003]:

**(a) Indie developer / advanced homelabber.** Runs a home server, often behind NAT, and wants deploys, domains and TLS without opening ports or fighting certificates. For this user the ingress answer is Cloudflare Tunnel [D-0024].

**(b) Small startup on its own hardware or VPS.** Has a public IP and wants a Railway/Vercel-like experience on infrastructure it controls [D-0003]. For this user the ingress answer is a reverse proxy with ACME [D-0024]. They care about reliability: app replicas for stateless apps, a warm copy of stateful data, and an honest statement of what each policy costs [D-0020, D-0021].

**(c) Team already operating with AI agents.** Already has agents doing operational work and wants those agents to act on infrastructure with scoped permissions, human approval for destructive steps, and a complete audit trail [D-0029, D-0030, D-0031].

Enterprise and managed customers are explicitly later [D-0003]. Nothing in the MVP is designed around them.

## 4. Goals

- **One interface for VMs and app deploys.** Create and manage VMs, and deploy services with push-to-deploy, domains, TLS and logs, from the same place [D-0002].
- **Agents as first-class operators from day one.** MCP and events are part of the product, not a later integration; the agent reads the same state, uses the same API and is logged the same way as a human [D-0002, D-0029, D-0030, D-0031, D-0032].
- **Apps keep running when the control plane dies.** Nodes are autonomous, store their desired state locally and reconcile it; they dial out to the control plane and never the reverse [D-0010, D-0011].
- **One binary, one-command install.** API, panel and engine ship in the same binary; the installer is a single command, and `POST /api/v1/setup` requires a one-time setup token [D-0012, D-0054, D-0033].
- **Works across VPS from different providers over the internet.** Independent nodes, not a tightly coupled cluster that needs low-latency links [D-0010].
- **Honest, explicit reliability trade-offs per app.** Each app carries an HA policy with its cost shown, and the system states when it cannot promise something [D-0020, D-0021].

## 5. Non-goals (for now)

The following are out of scope and each of them could eat the project [D-0034]:

- Live migration of running VMs [D-0034].
- Distributed storage such as Ceph [D-0034].
- Complex networking: SDN, VLANs [D-0034].
- Multi-tenancy with billing [D-0034].
- Writing our own VMM [D-0034, D-0007].

Also out of scope for now:

- Docker on nodes. Apps run in microVMs, not containers on the host [D-0008].
- Running our own image registry. A curated catalog in the repo points at pinned upstream images instead [D-0026].
- A container fallback engine in the MVP. That is a later `Engine` implementation [D-0009].
- Automatic failover before M6 [D-0016, D-0033].
- Persistent app data before M5a: local, non-replicated volumes ship in M5a [D-0052]; until then a deployed app has no persistent disk.

## 6. Product principles

**Agent-operable.** The agent is one more operator. It reads the same state as the panel, acts through the same service layer and API, and appears in the same audit log. MCP has no separate path and no separate permissions model [D-0002, D-0031].

**Least privilege, never forever.** Every agent action runs inside a grant: a scope, a resource selector and a TTL, optionally a maximum number of uses. Destructive or irreversible actions are off by default and need a human just-in-time approval that issues a short-lived extra grant [D-0030].

**Everything auditable.** The audit log is append-only and hash-chained, can be copied off-site to object storage with object lock, which ships in M5a [D-0031, D-0053], and records who acted, on whose behalf, triggered by what, through which tool or endpoint, with which parameters, with which result, and with the agent's stated justification [D-0031].

**Minimal, auditable dependencies.** Rust for everything that touches infrastructure, with every dependency justified [D-0056]. We write our own engine on Cloud Hypervisor and KVM rather than depending on Incus or writing a VMM; the engine sits behind an `Engine` trait so other backends stay possible [D-0007].

**Reliability honesty.** The data plane never depends on the panel [D-0011]. HA needs at least three votes, and Quoro says so instead of pretending a two-node setup is safe [D-0017]. The panel shows the cost of each HA policy [D-0020] and an N+1 capacity indicator [D-0021]. Failback is never automatic [D-0022].

**Self-hosted for real.** The static panel can be hosted anywhere, but the stateful API stays self-hosted [D-0012]. There is no vendor lock-in: DNS, LB and object storage sit behind provider interfaces, and Cloudflare is the first implementation, not the only one [D-0025].

**Supply-chain safety.** Binaries are signed and builds reproducible, because the installer is a pipe to a shell and the tool controls infrastructure [D-0027]. The catalog pins upstream images with verifiable signatures [D-0026].

## 7. Feature set mapped to milestones

Every milestone below is exactly as recorded in the roadmap decision; nothing is moved between milestones [D-0033].

| Milestone | Features | Key decisions |
|---|---|---|
| M0 | Contracts: repo, CI, cargo workspace, OpenAPI spec, DB schema, event schema, node-agent protocol including self-fencing; spike "boot a microVM from an OCI image" | [D-0033, D-0019, D-0056] |
| M1 | Skeleton: one-command installer, engine create/start/stop/delete, basic panel | [D-0033] |
| M2 | Deploy: node agent, deploy an OCI image with domain and TLS, live logs | [D-0033, D-0024] |
| M3 | AI-native: MCP read plus restart/redeploy, scoped grants, audit log, event webhooks | [D-0033, D-0029, D-0030, D-0031, D-0032] |
| M4 | Full experience: Git builds (Dockerfile; Railpack/Nixpacks to verify), metrics, web terminal, VM console | [D-0033, Q-0002] |
| M5a | Stateless app replicas + edge LB + backups to S3/R2 (cold) + local non-replicated volumes + off-site audit-log copy | [D-0033, D-0020, D-0023, D-0052, D-0053] |
| M5b | Witness, leases, fencing, replicated volumes (warm), capacity indicator | [D-0033, D-0017, D-0019, D-0020, D-0021] |
| M6 | Automatic failover + hot | [D-0033, D-0016, D-0020] |

HA and backups are out of the MVP, but the MVP architecture prepares them [D-0033]. For example, self-fencing is part of the node-agent protocol from M0 so it is not redesigned later [D-0019].

Which milestone is the first public release is open (Q-0006): options are the M3 demo or M4 complete.

Definitions of Done for each milestone live in `docs/product/roadmap.md`.

## 8. The AI-native loop

The product is built around one loop: detect, notify, wake, act [D-0032].

**Detect.** Each node agent plus health checks report state and metrics [D-0032]. Because telemetry stays on the node and is queried through the primary on demand, detection does not depend on shipping logs to a central place [D-0014].

**Notify.** Webhooks are configurable per event. Examples from the decision: VM down, build failed, disk full, certificate expiring [D-0032].

**Wake.** The webhook starts an agent, for example one built on the Claude Agent SDK, with Quoro's MCP server and the incident context attached [D-0032]. The reference runner lives in `examples/agent-runner/`, outside the quoro binary: it verifies the webhook signature, holds the agent's own key and acts through MCP within the incident grant [D-0049]. The agent is a registered identity with a name, a model and an owner [D-0029].

**Act.** The agent can do only what its grant allows. A destructive step requires human approval in the panel, after which the agent receives a short-lived extra grant [D-0030]. Every call lands in the audit log through the same service layer a human uses [D-0031].

### Worked example

The demo that explains the product in one sentence: "I told an agent the app was down, it restarted it, and it is in the audit log" [D-0033].

Grant: `app:restart` on `project=blog`, TTL one hour [D-0030]. Note that this is an incident-scoped grant: when an event woke the agent it received only the scopes needed, and the grant expires on its own [D-0030].

The resulting audit entry contains [D-0031]:

| Field | In this example |
|---|---|
| Who | the agent identity (name, model) [D-0029] |
| On behalf of whom | the human who authorized the agent [D-0029] |
| Triggering event | the "VM down" webhook that woke it [D-0032] |
| Tool / endpoint | the MCP tool or API endpoint for restart [D-0031] |
| Parameters | `project=blog`, target app |
| Result | success with the new app state |
| Agent's stated justification | free text the agent supplied [D-0031] |

Revoking the grant locks the agent out within seconds, and because the log is hash-chained any later tampering with the entry is evident [D-0030, D-0031].

## 9. Reliability, HA and the witness

### 9.1 Architecture recap

Nodes are independent and orchestrated by Quoro, not clustered Incus-style, so that a deployment can span VPS from different providers over the internet [D-0010]. Nodes are autonomous and dial out to the control plane; each stores its desired state locally and reconciles it, so apps keep running and self-restart if the control plane disappears [D-0011].

Every node runs the same binary containing API, panel and engine. Roles are `node`, `witness` and `edge`, and they are combinable [D-0012]. Exactly one node is the **primary**: the single source of truth for state. The other nodes are **replica nodes**: they hold a copy, serve reads and forward writes to the primary [D-0012]. State moves as a continuous, append-only, numbered, hash-chained log; a replica node asks for everything after N and verifies the chain, which makes tampering evident [D-0013].

Telemetry — app logs and metrics — stays on each node and is not replicated; the panel queries it through the primary on demand, with optional archive to object storage [D-0014].

### 9.2 Leader election roadmap

Single node in the MVP; manual promotion in M5 (which half, M5a or M5b, is open: Q-0024); automatic failover with Raft and at least three votes later, in M6 [D-0016, D-0033]. Manual promotion means a command or a button, or an agent doing it with human approval [D-0016]. Automatic election with only two nodes causes split-brain, because after a network cut both sides can believe they are primary [D-0016].

### 9.3 Why three votes, and the witness

Majority voting needs an odd number of voters. With two nodes Quoro tells the user that automatic failover is not safe, and offers a witness [D-0017]. The witness only breaks ties and stores no data [D-0017].

Witness options [D-0017]:

1. **Self-hosted lightweight daemon**, free, on any machine in a different failure domain. The failure domain matters: a home PC sitting next to node A fails together with node A and is not a witness.
2. **Optional external lease arbiter**: a Cloudflare Durable Object (single-threaded actor with strongly consistent storage and alarms; needs a Cloudflare account and a deployed Worker, and fencing tokens are still required) or S3/R2 conditional writes (atomic `If-Match`/`If-None-Match` PUT with one winner, 1 write/s per key, no TTL and fencing tokens required; see `docs/research/durable-objects.md` and `docs/research/r2-conditional-writes.md`).

> **Free-witness rule.** The self-hosted witness is free and first-class [D-0017]. A user with two nodes, a self-hosted witness and nothing else must be able to run a correct HA setup, and the panel must not hint otherwise.

### 9.4 Edge as the third voter

The edge/LB can be the third voter by combining the `witness` and `edge` roles in the same binary [D-0018]. It sits in a third location, sees both nodes, and decides where traffic goes, so its health view is the one users actually experience [D-0018]. The natural shape is two VPS plus one small LB VPS [D-0018].

Caveat: a single edge is a traffic single point of failure [D-0018]. Mitigations are Cloudflare as the traffic edge with the small VPS acting only as witness, or two edges with DNS failover [D-0018].

### 9.5 Fencing

Fencing is mandatory and lease-based, and the node agent self-fences from M0 [D-0019]. A lease is a node's right to run a stateful app. `expiring` leases (`warm`/`hot`) end when they are not renewed in time; `local` leases (`none`/`cold`) end only by an explicit revoke or an operator declaring the node lost [D-0047]. When the arbiter is unreachable an `expiring` lease first becomes `uncertain` (keep running, start nothing new) and only then `expired` (self-fence); see `docs/contracts/node-agent-protocol.md` §8. The reason is concrete: starting a stateful app on node B while it still runs, isolated, on node A produces two diverging copies and therefore data corruption [D-0019]. The same lease mechanism serves both primary election and per-app failover, which is why it has to be in the node-agent protocol from the beginning instead of being retrofitted later [D-0019].

### 9.6 HA policies

Each app carries one policy, and the panel shows what it costs [D-0020].

| Policy | What exists on other nodes | RPO | RTO | Cost |
|---|---|---|---|---|
| `none` | Nothing | Not applicable | Not applicable | No reserved resources (inferred, pending Q-0025) |
| `cold` | Backups only | Since the last backup | Restore time | Backup storage (inferred, pending Q-0025) [D-0023] |
| `warm` | Data continuously replicated, image pulled, instance stopped | Seconds | MicroVM boot | Disk |
| `hot` | Instance running and replicating | Near zero | Near zero | Disk + RAM + CPU |

Replication is asynchronous by default; synchronous is optional for nearby nodes and adds latency to every write [D-0020].

Suggested defaults [D-0020]: stateless apps get app replicas behind the edge LB when there are at least two nodes; stateful apps get `warm`. For stateless apps, the edge LB stops sending traffic to the dead node, so the user sees zero downtime [D-0020].

The `warm` RTO is the microVM boot time. Cloud Hypervisor's own claim of a boot "under 100 ms" is unbenchmarked and stays a **Hypothesis** (see `docs/research/cloud-hypervisor.md`); the design assumes fast boot makes `warm` nearly as good as `hot` [D-0008].

The N+1 capacity indicator answers a different question in plain language: "if node A dies, do its apps fit on the others?" For example, "yes", or "no, you are missing 6 GB of RAM" [D-0021]. No self-hosted tool does this well today, and it is an ideal signal for an agent to watch [D-0021].

### 9.7 Sync engine

The sync engine is ours: control-plane log replication plus a content-addressed chunk engine for volumes [D-0023]. A volume is snapshotted, split into content-addressed chunks — deduplicated and encrypted — and only new chunks are shipped; to another node this is `warm` replication, to S3 or R2 it is a `cold` backup [D-0023]. The first implementation uses incremental snapshots every few seconds, so RPO equals the interval [D-0023].

Databases are the exception: they use their native replication orchestrated by Quoro — Postgres streaming, Litestream for SQLite — offered as catalog templates, because block-level copies of a running database are only crash-consistent [D-0023] (Litestream 0.5 has no client-side encryption; the option is open, Q-0026; see `docs/research/rust-api-and-mcp-tooling.md`).

The snapshot primitive underneath the chunk engine is open (Q-0004); options are our own block diff and dirty tracking, or lvm-thin, ZFS or btrfs send.

### 9.8 Failback and planned switchover

Failback is never automatic or immediate [D-0022]. When node A comes back it does not have the data B collected while A was down, and A may also hold writes B never received, so there are two histories [D-0022].

The sequence [D-0022]:

1. A returns fenced, checks its lease, and starts nothing stateful.
2. A rejoins following B, which is now the source of truth: for control-plane state A becomes a replica node, and for each stateful app A holds the `warm` copy while B is the lease holder.
3. A's divergent writes are quarantined in a separate snapshot and recorded in the audit log. They are never silently dropped.
4. Incremental resync from the last common point; A becomes `warm`.
5. Default: no failback. The roles simply swap and B stays the lease holder.
6. If A is preferred, the operator runs a **planned switchover** ("relevo inteligente"): wait for replication lag near zero, wait for a low-traffic window chosen by the user or proposed by an agent with approval, freeze writes for seconds, run the final sync, move the lease, shift traffic at the LB, drain connections.

A planned switchover loses no data. Data loss of seconds happens only in surprise failures [D-0022]. For stateless apps the LB slow-starts traffic back to A [D-0022].

## 10. Ingress and providers

Two ingress paths cover the two main users [D-0024]: a reverse proxy with ACME for hosts with a public IP, and Cloudflare Tunnel for homelabs behind NAT, which avoids opening ports and fighting TLS.

The proxy implementation is open (Q-0001); options are driving Caddy through its admin API, the proven option, or embedding a Rust reverse proxy. Both sit behind a common interface [D-0024].

Multiple cloudflared replicas on the same tunnel give host-level failover only; whether they expose any app health check is a **Hypothesis** to be confirmed, and per-node failover needs one tunnel per node behind a load balancer. Cloudflare Load Balancing is a paid add-on from $5/month, its HTTPS monitors with the app hostname test the app, there are no TCP monitors for tunnels, and an LB sees all replicas of one tunnel as one endpoint; see `docs/research/cloudflare-lb-and-tunnel.md` [D-0024].

Providers sit behind interfaces: DNS, load balancing and S3-compatible storage, with Cloudflare as the first implementation and not the only one [D-0025]. Backups can target any S3-compatible store, including R2, B2 and MinIO [D-0025].

## 11. Platform requirements

The MVP requires a KVM-capable host: bare metal, or a VPS that exposes nested virtualization [D-0009]. Many VPS do not expose nested virtualization, which is exactly why the requirement is stated up front rather than discovered at install time [D-0009]. A container fallback is a later `Engine` implementation and is more work with weaker isolation, so it is not a way around the requirement [D-0009].

Nested virtualization is the exception on cheap VPS: it is not available on Hetzner Cloud, Hostinger, Contabo VPS or netcup VPS, while it is available (some opt-in) on AWS 7i/8i families, GCP Intel, Azure Dv3/Dv4, OCI Intel VMs, OVHcloud Public Cloud, Contabo VDS, Vultr VX1 and DigitalOcean. Bare metal is the reliable path, and the installer must always probe `/dev/kvm` with a real microVM boot rather than trusting the provider's documentation; see `docs/research/vps-nested-virt.md`.

Cloud Hypervisor is v54.0 under Apache-2.0 AND BSD-3-Clause, driven through an HTTP/JSON API over a UNIX socket, and the host kernel must be 6.8 or newer; snapshots hold RAM and device state but not disks, with no incremental API, virtio-fs needs an external virtiofsd, and consoles are text-only (see `docs/research/cloud-hypervisor.md`). The console is therefore a text console over the microVM's serial or virtio-console, shown with xterm.js (Cloud Hypervisor has no VNC or framebuffer); consoles are text-only and there is no noVNC in the stack [D-0056].

## 12. Managed offering

An optional managed offering may come later [D-0003]; nothing in it is designed yet and no feature in this document depends on it. The self-hosted witness is free and first-class [D-0017].

## 13. Competitive landscape

Approximate stacks; verify before any pitch [F-0006].

| Product | Layer | Stack (approx.) | Gap Quoro addresses |
|---|---|---|---|
| Proxmox VE | Hypervisor | Perl + Rust, ExtJS UI | No app deploy, no modern UI, no official MCP [F-0006] |
| Incus | Hypervisor | Go, React UI | No app deploy, no official MCP [F-0006] |
| Openship | Deploy | Bun/Hono, Next.js | No own VMs, no agent-native design [F-0006] |
| Coolify / Dokploy | Deploy | PHP / TypeScript | No own VMs, no agent-native design [F-0006] |
| Harvester + Rancher | Both layers | Kubernetes + KubeVirt | Heavy, hosting-provider focused [F-0006] |
| Cozystack | Both layers | Kubernetes + KubeVirt | Heavy, hosting-provider focused [F-0006] |
| Ubicloud | Both layers | Ruby | Aims to be a public cloud alternative [F-0006] |

Proxmox and Incus MCPs exist, but they are small community projects [F-0006].

Where Quoro sits: it covers both layers — hypervisor and app deploy — without Kubernetes, and it is agent-native by design rather than by integration [D-0002, F-0006]. The closest shape is "Proxmox plus Dokploy in one tool", with a safety model for agents built into the core [D-0002, D-0029].

## 14. Risks

| Risk | Why it matters | Mitigation |
|---|---|---|
| Scope creep from HA, storage and networking | Each area could consume the project on its own [F-0007] | Milestones with fixed contents [D-0033]; HA out of the MVP; explicit non-goals [D-0034] |
| Rust iteration speed for CRUD and auth | Slower product iteration than starting those parts in TypeScript [F-0007] | Open, to be settled in the architecture doc; the original "Rust for infra, TS for the rest" position was later replaced by Rust and minimal dependencies everywhere [F-0007, D-0056] |
| Engine integration maintenance | An external engine brings ongoing integration and upgrade work [F-0007] | The engine sits behind an `Engine` trait, so backends can be swapped or added [D-0007] |
| Agents with excess permissions deleting real infrastructure | An over-scoped agent can destroy production | Scoped TTL grants, JIT approval for destructive actions, audit log, and microVM isolation [D-0008, D-0030, D-0031] |
| Two-node split-brain | Both nodes can believe they are primary after a network cut [D-0016] | No automatic election below three votes; Quoro tells the user and offers a witness [D-0016, D-0017] |
| Dependency on KVM-capable hosts | Many VPS do not expose nested virtualization [D-0009] | State the requirement up front and probe `/dev/kvm` with a real microVM boot rather than trusting provider docs (`docs/research/vps-nested-virt.md`); container fallback deferred [D-0009] |

## 15. Open questions

All of the following are open. This document does not choose between the options.

| Id | Question | Options (if known) |
|---|---|---|
| Q-0001 | Ingress proxy implementation | Caddy driven through its admin API; embedded Rust reverse proxy |
| Q-0002 | Git build method | Dockerfile only; Railpack; Nixpacks (verify current state of each) |
| Q-0003 | OCI image to microVM rootfs conversion and minimal init | Write our own; reuse an existing component |
| Q-0004 | Snapshot primitive under the chunk engine | Own block diff / dirty tracking; lvm-thin; ZFS; btrfs send |
| Q-0005 | Node mesh | Kernel WireGuard driven by Quoro; userspace such as boringtun; own implementation |
| Q-0006 | First public release | The M3 demo; M4 complete |
| Q-0008 | Domain registration and trademark check (IMPI/USPTO) | Before going public with the name |
| Q-0009 | Docs site tooling | Not decided |
| Q-0024 | Which half of M5 owns manual promotion of a replica node (D-0016 says M5; D-0033 lists it in neither M5a nor M5b) | M5a; M5b; a separate item before M5a |
| Q-0025 | HA policy costs for `none` and `cold` as stated in this document | Confirm or correct the inferred "no reserved resources" and "backup storage" costs |
| Q-0026 | Encrypted SQLite backups now that Litestream 0.5 has no client-side encryption | Accept SSE-C; pin Litestream 0.3.x with age; back up with Quoro's own encrypted chunk engine; an encrypting S3 proxy |

Realtime transport is decided: SSE carries one-way streams (event stream, live logs, deployment progress) and WebSockets carry bidirectional sessions (terminal, VM console); see `docs/architecture/architecture.md` §13 [D-0057].

### Answered verification questions

| Id | Topic | Answer in |
|---|---|---|
| Q-0010 | Cloud Hypervisor license, REST API surface, boot time, snapshot/restore, virtio-fs, console | `docs/research/cloud-hypervisor.md` |
| Q-0011 | Cloudflare R2 conditional writes usable as a lease arbiter | `docs/research/r2-conditional-writes.md` |
| Q-0012 | Cloudflare Durable Objects as external lease arbiter (consistency, cost) | `docs/research/durable-objects.md` |
| Q-0013 | Cloudflare Load Balancing pricing and health checks; cloudflared multi-replica failover | `docs/research/cloudflare-lb-and-tunnel.md` |
| Q-0014 | rmcp maturity; Litestream replication to R2; utoipa + orval/openapi-ts pipeline | `docs/research/rust-api-and-mcp-tooling.md` |
| Q-0015 | Which common VPS providers expose nested virtualization / KVM | `docs/research/vps-nested-virt.md` |

## 16. References

- `docs/product/roadmap.md` — milestones and Definitions of Done.
- `docs/glossary.md` — terms such as primary, replica node, app replica, lease holder, HA policies and node roles.
- Decision ledger (private) — the source of truth for every id cited here.
