# Glossary

Terms used across Quoro's documents. Bracketed ids point to the decision ledger; where a definition and an accepted decision disagree, the decision wins.

**ACME**: The protocol the ingress uses to obtain and renew TLS certificates automatically for hosts that expose a public IP. [D-0024]

**ADR**: Architecture decision record, one per accepted decision; an accepted ADR is law and changing it requires a new ADR approved by the maintainer. [D-0036, D-0040]

**agent**: An AI actor with its own identity (name, model, owner), separate from humans; every action records who acted, on behalf of whom, and why. [D-0029]

**AI-native loop**: The cycle detect (node agent and health checks report state) -> notify (configurable webhooks per event) -> wake (a webhook starts an agent with Quoro's MCP and incident context) -> act (only within the agent's grant). [D-0032]

**app replica**: An extra running instance of a stateless app on another node, behind the edge load balancer, so the edge can stop sending traffic to a dead node and keep the app available. [D-0020]

**approval request**: A request an agent files for a destructive or irreversible action, which a human approves in the panel; approval releases a short-lived extra grant. Also: JIT approval. [D-0030]

**audit log**: An append-only, hash-chained record of every action, holding who, on behalf of whom, the triggering event, the tool or endpoint, the parameters, the result and the agent's stated justification. It can be copied off-site to S3/R2 with object lock, and MCP writes through the same service layer and the same log. [D-0031]

**catalog**: Curated templates kept in the repository, pointing to pinned upstream images with verifiable signatures; Quoro runs no image registry of its own for now. Its format, owner, location and signature scheme are decided at M4 planning, before the first template ships. Also: template. [D-0026, D-0070]

**change log**: The numbered, append-only, hash-chained sequence of state changes that a replica node asks for ("give me everything after N") and verifies by hash. Also: state log. [D-0013, D-0015]

**chunk engine**: The volume sync mechanism: a snapshot is split into content-addressed, deduplicated, encrypted chunks and only new chunks are shipped. Shipping to another node is warm replication; shipping to S3/R2 is cold backup. [D-0023]

**Cloudflare Tunnel**: The ingress option for homelabs behind NAT, where cloudflared dials out to Cloudflare so no port has to be opened. Load Balancing is a paid add-on from $5/month and an LB sees all replicas of one tunnel as one endpoint; cloudflared replicas give host-level failover only, per-node failover needs one tunnel per node behind an LB, and whether replicas expose any app health check is a **Hypothesis** (see `docs/research/cloudflare-lb-and-tunnel.md`). [D-0024]

**Cloud Hypervisor**: The Rust virtual machine monitor, driven through its HTTP/JSON API over a UNIX socket, that Quoro's engine builds on together with KVM. It is v54.0 under Apache-2.0 AND BSD-3-Clause; snapshots hold RAM and device state but not disks, with no incremental API; virtio-fs needs an external virtiofsd; consoles are text-only (serial, virtio-console), with no VNC or GPU; the host kernel must be 6.8 or newer (see `docs/research/cloud-hypervisor.md`; its "under 100 ms" boot claim is unbenchmarked and stays a **Hypothesis**). [D-0007]

**cold**: HA policy where other nodes hold backups only, so RPO is the time since the last backup and RTO is a restore. No copy of the app runs elsewhere, so a failure means restoring from backup. Its cost is backup storage and restore time (D-0068). [D-0020]

**console**: A terminal in the panel for a microVM, text-only, rendered with xterm.js; there is no graphical console and no noVNC. [D-0056]

**contract**: A frozen interface: OpenAPI spec, event schema, DB schema, node-agent protocol, audit-log format, permission model, Engine trait. Contracts freeze after M0 and any change is a standalone PR reviewed on its own. [D-0033, D-0036, D-0040]

**control plane**: The stateful layer (API and state) that holds Quoro's state and accepts writes through the primary; if it dies, apps keep running and only changes are blocked. [D-0010, D-0011]

**data plane**: The apps and node agents that keep running and reconciling their local desired state when the panel or the control plane is unreachable. [D-0010, D-0011]

**desired state**: The locally stored description of what a node should run, written by the control plane and applied by the node agent without needing the control plane to be reachable. [D-0011]

**dial out**: The rule that a node opens the connection to the control plane and never the reverse, so the control plane can live anywhere and nodes work behind NAT. [D-0011]

**edge**: Node role that is the traffic entry point and load balancer; it can serve as the third voter, and a single edge is a traffic single point of failure. [D-0012, D-0018]

**Engine trait**: The boundary behind which the Cloud Hypervisor engine sits, so other backends such as Firecracker or containers can be added later. [D-0007, D-0009]

**external lease arbiter**: An optional off-cluster service that grants leases, either a Cloudflare Durable Object (single-threaded actor with strongly consistent storage and alarms; needs a Cloudflare account and a deployed Worker, and fencing tokens are still required) or S3/R2 conditional writes (atomic `If-Match`/`If-None-Match` PUT with one winner, 1 write/s per key, no TTL and fencing tokens required; not portable, since AWS S3 and MinIO have it while Backblaze B2 appears to lack it, a **Hypothesis**; see `docs/research/durable-objects.md` and `docs/research/r2-conditional-writes.md`). [D-0017]

**failback**: Returning an app to its previous node after a failure; it is never automatic or immediate, and the default is no failback, with roles simply swapped. [D-0022]

**failure domain**: A set of machines that fail together, for example a home PC next to node A; a witness must sit outside the nodes' failure domain. [D-0017]

**fencing**: A node's mandatory, lease-based act of stopping its own stateful apps when it loses its lease — for an `expiring` lease only once it is `expired`, not while it is `uncertain`; the node agent must self-fence from M0 so the protocol is not redesigned later. [D-0019]

**grant**: A permission envelope made of scope, resource selector, TTL and optionally a maximum number of uses, for example `app:restart` on `project=blog` for one hour. Tokens are short-lived and refreshed against the grant, and revoking the grant locks the agent out within seconds. [D-0030]

**HA policy**: The per-app reliability choice among `none`, `cold`, `warm` and `hot`, with its cost shown in the panel. Replication is asynchronous by default (synchronous is optional for nearby nodes and adds latency to every write), and suggested defaults are app replicas behind the load balancer for stateless apps on two or more nodes and `warm` for stateful apps. [D-0020]

**hot**: HA policy where the app runs and replicates on other nodes, giving near-zero RPO and RTO at the cost of disk, RAM and CPU. [D-0020]

**incident grant**: A grant with just the scopes needed for the incident that woke an agent, issued when an event wakes it and expiring automatically. [D-0030]

**ingress**: The entry layer for app traffic: a reverse proxy with ACME for hosts with a public IP, Cloudflare Tunnel for hosts behind NAT, both behind a common interface; the proxy is an embedded Rust proxy (hyper + rustls with an ACME client crate), with Caddy driven through its admin API kept as a fallback backend (D-0059). [D-0024, D-0059]

**JIT approval**: See approval request. [D-0030]

**lab**: The test lab is three nested-virtualization Debian 13 VMs (lab-1, lab-2 and lab-3) on a single physical host, so they share one failure domain; it is not valid for performance benchmarks. [D-0036]

**lease**: A lease is a node's right to run a stateful app. `expiring` leases (`warm`/`hot`, from M5b) end when they are not renewed in time; `local` leases (`none`/`cold`) never expire when the arbiter is unreachable and end only by `lease_revoke` or an operator declaring the node lost. When the arbiter is unreachable an `expiring` lease first becomes `uncertain` (keep running, start nothing new) and only then `expired` (self-fence); see `docs/contracts/node-agent-protocol.md` §8. [D-0019, D-0047]

**lease holder**: The node currently holding an app's lease; only it may run that stateful app. [D-0019]

**majority**: See quorum. [D-0016, D-0017]

**manual promotion**: The M5b action (D-0067) that promotes a replica node to primary, either as a command or button or by an agent with human approval. [D-0016, D-0067]

**MCP**: Model Context Protocol server exposing Quoro to agents; it is first-class from day one and uses the same service layer and audit log as the HTTP API. [D-0002, D-0031]

**mesh**: The WireGuard network connecting nodes across providers, configured by Quoro over netlink against kernel WireGuard with no `wg-quick`, per-node keypairs and peers in desired state (D-0063). [D-0010, D-0063]

**microVM**: The unit of app isolation, one per app, booted with a minimal init; nodes run no Docker. [D-0008]

**milestone**: A roadmap stage (M0, M1, M2, M3, M4, M5a, M5b, M6, then Later) that ends with a demo on the lab and the maintainer's approval before the next one starts. [D-0033, D-0036]

**minimal init**: The small init process that boots inside a microVM in place of a full distribution init; Quoro writes its own minimal Rust init (D-0061). [D-0008, D-0061]

**N+1 capacity indicator**: The panel answer to "if node A dies, do its apps fit on the others?", given as yes or as no plus what is missing, for example 6 GB of RAM. [D-0021]

**node**: (1) A machine running the Quoro binary; (2) the node role, as opposed to witness or edge. [D-0012]

**node agent**: The per-node component that stores desired state locally, reconciles it, dials out to the control plane and self-fences when it loses its lease (for an `expiring` lease, only once it is `expired`). [D-0011, D-0019]

**none**: HA policy where nothing runs on other nodes, so no resources are reserved elsewhere (D-0068) and no RPO or RTO is promised for the app. [D-0020]

**OCI image**: The container image a deploy is built from; Quoro converts it into a root disk and boots it in a microVM, through its own OCI-to-ext4 pipeline with layer paths resolved inside the rootfs (D-0061). [D-0008, D-0061]

**planned switchover**: A deliberate handover of an app to another node: wait for replication lag near zero and a low-traffic window, freeze writes briefly, do a final sync, move the lease, shift traffic at the load balancer and drain connections. It loses no data, unlike a surprise failure, which can lose seconds. Also: relevo, "relevo inteligente". [D-0022]

**primary**: The one control-plane node that is the single source of truth; all writes go through it. [D-0012]

**provider**: The credentials of one external provider, stored as a resource with a `prv_` id and one kind (`dns`, `tunnel`, `lb` or `object_store`). The secret is write-only, encrypted with the cluster data-encryption key, never returned by a read and redacted from audit parameters; managing providers needs the human-only scope `provider:manage`, and there is no MCP tool for it. The credential for the off-site audit copy is a separate put-only `object_store` provider. [D-0051]

**provider interface**: The pluggable boundary for DNS, load balancing and S3-compatible storage, with Cloudflare as the first implementation rather than the only one. [D-0025]

**quarantined snapshot**: The separate snapshot in which a returning node's divergent writes are kept and recorded in the audit log, so they are never silently dropped. [D-0022]

**quorum**: The majority of votes needed for automatic decisions; HA requires at least three votes and an odd number of voters. Also: majority, vote. [D-0016, D-0017]

**Raft**: The consensus algorithm planned for automatic failover with a majority of at least three votes, in M6. [D-0016]

**reconcile**: See desired state. [D-0011]

**relevo**: See planned switchover. [D-0022]

**replica**: (1) replica node: a node that holds a copy of control-plane state, serves reads and forwards writes to the primary; (2) app replica: an extra running instance of a stateless app behind the edge load balancer. A replica node is not an app replica. [D-0012, D-0020]

**reproducible build**: A build that can be rebuilt bit for bit from its sources; reproducible builds and signed binaries are mandatory because the installer is `curl | sh`. Also: signed binary. [D-0027]

**role**: What a node does in the cluster (node, witness, edge), combinable in the same binary. [D-0012]

**rootfs**: The root disk a microVM boots from, produced from an OCI image at deploy time. [D-0008]

**RPO**: Recovery point objective, the amount of data an app may lose on failure; each HA policy sets it, from "since the last backup" for `cold` to near zero for `hot`. [D-0020]

**RTO**: Recovery time objective, how long an app takes to serve again after failure; each HA policy sets it, from a restore for `cold` to a microVM boot for `warm` and near zero for `hot`. [D-0020]

**scope**: The verb inside a grant, for example read, restart, redeploy or delete. [D-0030]

**self-fencing**: See fencing. [D-0019]

**setup token**: A one-time secret required in the `Quoro-Setup-Token` header by `POST /api/v1/setup`, which claims the owner on first run. It is single use and expires after one hour; a missing or invalid token answers `401 unauthenticated`. [D-0054]

**signed binary**: See reproducible build. [D-0027]

**split-brain**: The state where both sides of a network cut become primary and diverge, which is why automatic election with only two nodes is not allowed. [D-0016]

**stateful app**: An app that keeps data of its own, so it may run only on its lease holder and is fenced off when the lease is lost. [D-0019, D-0020]

**stateless app**: An app that keeps no local state, so it can run as app replicas on several nodes behind the edge load balancer for zero downtime. [D-0020]

**state log**: See change log. [D-0013, D-0015]

**sync engine**: Quoro's own replication layer, made of control-plane log replication and the chunk engine for volumes; databases instead use their native replication, offered as catalog templates (Litestream 0.5 has no client-side encryption, so SQLite backups use Quoro's own chunk engine from consistent snapshots instead, D-0069; a Litestream-based lower-RPO backend may be added later as an option). [D-0023, D-0069]

**telemetry**: App logs and metrics, which stay on each node and are queried through the primary on demand, with optional archive to S3/R2; only state is replicated. [D-0014]

**template**: See catalog. [D-0026]

**TTL**: Time to live, the expiry attached to a grant after which it no longer authorizes anything. [D-0030]

**vote**: See quorum. [D-0016, D-0017]

**voter**: See quorum. [D-0016, D-0017]

**warm**: HA policy where data is continuously replicated and the image is pulled on another node but the microVM stays stopped, giving RPO in seconds and RTO equal to a microVM boot at the cost of disk. [D-0020]

**warm copy**: The stopped, continuously replicated copy of a stateful app on another node under the `warm` policy; it may start only once the previous lease holder has lost the app's lease. [D-0019, D-0020]

**witness**: Node role that only breaks ties and stores no data, run as a self-hosted daemon in a failure domain different from the nodes. The self-hosted witness is free and first-class [D-0017].

**workstream**: A parallel line of work after M0, each in its own crate or package and owning its paths: engine, control-plane, panel, node-agent, ingress, mcp, installer/releases/docs site, and later sync/chunk engine and witness/edge (see `docs/architecture/architecture.md` §17). [D-0036]
