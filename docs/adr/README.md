# Architecture Decision Records

These ADRs mirror the accepted decisions of the Quoro project ledger. They are generated from the maintainer's decision ledger; do not edit them by hand. To change a decision, open an issue proposing a new decision that supersedes it; once the maintainer accepts it, a new ADR is generated.

Some decisions about internal coordination or business planning are kept private, so numbering has gaps.

| ADR | Title | Status | Tags | Date |
| --- | --- | --- | --- | --- |
| [0001](0001-project-name-is-quoro.md) | Project name is Quoro | Accepted | product | 2026-11-02 |
| [0002](0002-quoro-unifies-hypervisor-and-app-deploy-in-one-self-hosted.md) | Quoro unifies hypervisor and app deploy in one self-hosted tool, operable by AI agents | Accepted | product | 2026-11-02 |
| [0003](0003-start-as-a-self-use-experimenter-tool-enterprise-or-managed.md) | Start as a self-use / experimenter tool; enterprise or managed offering later | Accepted | product | 2026-11-02 |
| [0004](0004-license-agpl-3-0-core-enterprise-code-in-ee-under-a.md) | License: AGPL-3.0 core, enterprise code in ee/ under a separate license, CLA from the first external contributor | Accepted | product, oss | 2026-11-02 |
| [0005](0005-repos-public-quoro-repository-english-private-coordination.md) | Repos: public quoro repository (English) + private coordination repository | Accepted | product, process | 2026-11-02 |
| [0007](0007-own-engine-on-cloud-hypervisor-kvm-not-incus-not-an-own-vmm.md) | Own engine on Cloud Hypervisor + KVM (not Incus, not an own VMM) | Accepted | engine, architecture | 2026-11-02 |
| [0008](0008-one-microvm-per-app-no-docker-on-nodes.md) | One microVM per app; no Docker on nodes | Accepted | engine, architecture | 2026-11-02 |
| [0009](0009-mvp-requires-a-kvm-capable-host-container-fallback-is-a.md) | MVP requires a KVM-capable host; container fallback is a later Engine implementation | Accepted | engine, product | 2026-11-02 |
| [0010](0010-independent-nodes-orchestrated-by-quoro-dokploy-style-not.md) | Independent nodes orchestrated by Quoro (Dokploy-style), not Incus-style clustering | Accepted | architecture, node-agent | 2026-11-02 |
| [0011](0011-nodes-are-autonomous-and-dial-out.md) | Nodes are autonomous and dial out | Accepted | architecture, node-agent, reliability | 2026-11-02 |
| [0012](0012-same-binary-on-every-node-one-primary-is-the-single-source.md) | Same binary on every node; one primary is the single source of truth, others are replicas | Accepted | architecture, control-plane, reliability | 2026-11-02 |
| [0013](0013-state-replication-is-continuous-over-an-append-only.md) | State replication is continuous over an append-only, numbered, hash-chained log | Accepted | architecture, control-plane, reliability | 2026-11-02 |
| [0014](0014-telemetry-app-logs-metrics-stays-on-each-node-only-state-is.md) | Telemetry (app logs, metrics) stays on each node; only state is replicated | Accepted | architecture, observability | 2026-11-02 |
| [0015](0015-sqlite-first-state-stored-as-a-numbered-change-log.md) | SQLite first; state stored as a numbered change log | Accepted | architecture, control-plane | 2026-11-02 |
| [0016](0016-leader-election-roadmap-single-node-in-mvp-manual-promotion.md) | Leader election roadmap: single node in MVP, manual promotion in M5, Raft auto-failover with >=3 votes later | Accepted | reliability, control-plane | 2026-11-02 |
| [0017](0017-ha-requires-at-least-3-votes-with-2-nodes-quoro-tells-the.md) | HA requires at least 3 votes; with 2 nodes Quoro tells the user and offers a witness | Accepted | reliability, witness | 2026-11-02 |
| [0018](0018-the-edge-lb-can-be-the-third-voter-role-witness-edge-of-the.md) | The edge/LB can be the third voter (role witness+edge of the same binary) | Accepted | reliability, witness, ingress | 2026-11-02 |
| [0019](0019-fencing-is-mandatory-and-lease-based-node-agent-self-fences.md) | Fencing is mandatory and lease-based; node agent self-fences from M0 | Accepted | reliability, node-agent, fencing | 2026-11-02 |
| [0020](0020-per-app-ha-policies-none-cold-warm-hot-with-cost-shown-in.md) | Per-app HA policies: none / cold / warm / hot, with cost shown in the panel | Accepted | reliability, ha-policy | 2026-11-02 |
| [0021](0021-n-1-capacity-indicator-if-node-a-dies-do-its-apps-fit-on.md) | N+1 capacity indicator: 'if node A dies, do its apps fit on the others?' | Accepted | reliability, ha-policy, panel | 2026-11-02 |
| [0022](0022-failback-is-never-automatic-or-immediate-returning-node.md) | Failback is never automatic or immediate; returning node rejoins as replica and handover is planned | Accepted | reliability, failback | 2026-11-02 |
| [0023](0023-sync-engine-is-our-own-control-plane-log-replication-a.md) | Sync engine is our own: control-plane log replication + a content-addressed chunk engine for volumes | Accepted | sync, reliability, backups | 2026-11-02 |
| [0024](0024-ingress-reverse-proxy-with-acme-for-public-ip-hosts.md) | Ingress: reverse proxy with ACME for public-IP hosts, Cloudflare Tunnel for homelabs behind NAT | Accepted | ingress | 2026-11-02 |
| [0025](0025-provider-interfaces-dns-lb-s3-compatible-storage-with.md) | Provider interfaces (DNS, LB, S3-compatible storage) with Cloudflare as the first implementation, not the only one | Accepted | providers, oss | 2026-11-02 |
| [0026](0026-curated-catalog-in-the-repo-instead-of-running-an-own-image.md) | Curated catalog in the repo instead of running an own image registry (for now) | Accepted | catalog, security, oss | 2026-11-02 |
| [0027](0027-signed-binaries-and-reproducible-builds-are-mandatory.md) | Signed binaries and reproducible builds are mandatory | Accepted | security, release, oss | 2026-11-02 |
| [0028](0028-baseline-stack-to-be-confirmed-in-the-architecture-doc.md) | Baseline stack (to be confirmed in the architecture doc) | Superseded by 0056 | stack | 2026-11-02 |
| [0029](0029-agents-are-first-class-identities-separate-from-humans.md) | Agents are first-class identities, separate from humans | Accepted | agents, security | 2026-11-02 |
| [0030](0030-grants-scope-resource-selector-ttl-optional-max-uses.md) | Grants: scope + resource selector + TTL (+ optional max uses); incident-scoped; JIT approval for destructive actions | Accepted | agents, security, auth | 2026-11-02 |
| [0031](0031-audit-log-append-only-hash-chained-optionally-copied-to-s3.md) | Audit log: append-only, hash-chained, optionally copied to S3/R2 with object lock; MCP uses the same path | Accepted | agents, audit, security | 2026-11-02 |
| [0032](0032-ai-native-loop-detect-notify-wake-agent-act-within-limits.md) | AI-native loop: detect -> notify -> wake agent -> act within limits | Accepted | agents, events | 2026-11-02 |
| [0033](0033-milestones-m0-m6-ha-and-backups-out-of-mvp-but-the-mvp.md) | Milestones M0-M6; HA and backups out of MVP but the MVP architecture prepares them | Accepted | roadmap | 2026-11-02 |
| [0034](0034-out-of-scope-for-now.md) | Out of scope for now | Accepted | roadmap, scope | 2026-11-02 |
| [0036](0036-anti-drift-rules.md) | Anti-drift rules | Accepted | process | 2026-11-02 |
| [0037](0037-git-conventions.md) | Git conventions | Accepted | process, git | 2026-11-02 |
| [0040](0040-write-the-bases-before-any-code.md) | Write the bases before any code | Accepted | process, docs | 2026-11-02 |
| [0042](0042-repo-agents-md-must-carry-the-maintainer-s-working-rules.md) | Repo AGENTS.md must carry the maintainer's working rules, because non-Claude agents never see the maintainer's CLAUDE.md | Accepted | process, agents-md | 2026-11-02 |
| [0044](0044-realtime-transport-websockets-vs-sse-stays-open-until-the.md) | Realtime transport (WebSockets vs SSE) stays open until the architecture doc | Superseded by 0057 | roadmap, stack | 2026-11-02 |
| [0047](0047-leases-have-two-modes-local-ha-none-cold-never-expire-on.md) | Leases have two modes: local (HA none/cold, never expire on control-plane loss) and expiring (warm/hot, self-fence on expiry) | Accepted | reliability, node-agent, fencing | 2026-11-02 |
| [0048](0048-api-contract-source-of-truth-hand-written-docs-contracts.md) | API contract source of truth: hand-written docs/contracts/openapi.yaml is frozen; utoipa output is diffed against it in CI; orval generates the panel client | Accepted | stack, api | 2026-11-02 |
| [0049](0049-agent-runner-d-0032-wake-reference-runner-in-examples-agent.md) | Agent runner (D-0032 wake): reference runner in examples/agent-runner/, owned by the mcp workstream, outside the quoro binary | Accepted | agents, mcp, events | 2026-11-02 |
| [0050](0050-cluster-keys-separate-ed25519-signing-key-and-xchacha20.md) | Cluster keys: separate Ed25519 signing key and XChaCha20-Poly1305 data-encryption key, generated at setup, stored as 0600 files outside SQLite, plus a node-local key | Accepted | security, control-plane, node-agent | 2026-11-02 |
| [0051](0051-provider-credentials-a-human-only-providers-resource-api.md) | Provider credentials: a human-only providers resource (API + replicated table with secret_enc) added to the contract before the M0 freeze | Accepted | providers, ingress, security | 2026-11-02 |
| [0052](0052-local-persistent-volumes-non-replicated-ship-with-m5a.md) | Local persistent volumes (non-replicated) ship with M5a, before the cold-backup DoD; until then the PRD states no persistent app data | Accepted | engine, sync, roadmap | 2026-11-02 |
| [0053](0053-off-site-audit-log-copy-d-0031-ships-in-m5a-with-the-s3.md) | Off-site audit-log copy (D-0031) ships in M5a with the S3-compatible path, gated by an object-lock probe per provider | Accepted | audit, roadmap, security | 2026-11-02 |
| [0054](0054-host-layout-and-cli-one-tls-listener-for-api-panel-mcp-on.md) | Host layout and CLI: one TLS listener for API/panel/MCP on 8443, self-signed primary certificate pinned by join commands, /var/lib/quoro + /etc/quoro/quoro.toml, one-time setup token for /setup | Accepted | installer, control-plane, security | 2026-11-02 |
| [0055](0055-lab-tls-source-for-the-m2-deploy-dod-cloudflare-tunnel-with.md) | Lab TLS source for the M2 deploy DoD: Cloudflare Tunnel with a test domain; ACME exercised separately via DNS-01 against a test zone | Accepted | ingress, lab, roadmap | 2026-11-02 |
| [0056](0056-baseline-stack-without-novnc-consoles-are-text-only-xterm.md) | Baseline stack without noVNC: consoles are text-only (xterm.js) because Cloud Hypervisor has no VNC or display device | Accepted | stack, engine, console | 2026-11-02 |
| [0057](0057-realtime-transport-sse-for-one-way-streams-websockets-for.md) | Realtime transport: SSE for one-way streams, WebSockets for bidirectional sessions | Accepted | stack, architecture | 2026-11-02 |
