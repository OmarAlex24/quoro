# Quoro roadmap

Status: living document; the [status table](#status) below shows where each milestone stands. Source: D-0033 (milestones), D-0034 (out of scope), D-0036 (done = runs on the lab).

This turns D-0033 into a gated plan and adds no features: where another decision explains how an item behaves, the decision is cited next to the item, and every claim that rests on an unverified external fact is either answered in `docs/research/` or marked as a hypothesis.

> **Release callout: first public release (D-0064, D-0137).** v0.1.0 is an experimental preview with signed builds, not for production; it was tagged after the M4 demo and contains M3 and M4 [D-0137]. The first general release and the broad announcement are a later tag after the M4 close-out [D-0064, D-0137]. The domain is registered; the trademark search was waived [D-0137].

## Status

| Milestone | Status | Release | GitHub milestone |
|---|---|---|---|
| M0 — Contracts | Done | [v0.0.1](https://github.com/OmarAlex24/quoro/releases/tag/v0.0.1) | [M0](https://github.com/OmarAlex24/quoro/milestone/1) |
| M1 — Skeleton | Done | [v0.0.1](https://github.com/OmarAlex24/quoro/releases/tag/v0.0.1) | [M1](https://github.com/OmarAlex24/quoro/milestone/2) |
| M2 — Deploy | Done | [v0.0.2](https://github.com/OmarAlex24/quoro/releases/tag/v0.0.2) | [M2](https://github.com/OmarAlex24/quoro/milestone/3) |
| M3 — AI-native | Done | [v0.1.0](https://github.com/OmarAlex24/quoro/releases/tag/v0.1.0) | [M3](https://github.com/OmarAlex24/quoro/milestone/4) |
| M4 — Full experience | Done | [v0.1.0](https://github.com/OmarAlex24/quoro/releases/tag/v0.1.0) | [M4](https://github.com/OmarAlex24/quoro/milestone/5) |
| M5a — Stateless HA and backups | Done (demo approved on 2026-11-05) | [v0.2.0](https://github.com/OmarAlex24/quoro/releases/tag/v0.2.0) | [M5a](https://github.com/OmarAlex24/quoro/milestone/6) |
| M5b — Witness, leases and warm replication | In progress (plan approved 2026-11-06) | — | [M5b](https://github.com/OmarAlex24/quoro/milestone/7) |
| M6 — Automatic failover and hot | Planned | — | [M6](https://github.com/OmarAlex24/quoro/milestone/8) |

Status comes from each milestone's issues on GitHub, its Definition of Done below and the release tags. "Release" is the first tag that shipped the milestone. M4 still has two follow-up issues open in its GitHub milestone. What each release contains is in the [changelog](https://github.com/OmarAlex24/quoro/blob/main/CHANGELOG.md).

---

## 1. How to read this roadmap

### Gating rules

- A milestone starts only after the previous milestone's demo ran on the lab and the maintainer approved it [D-0036].
- "Done" means it runs on the lab, not that it compiles [D-0036].
- Contracts (OpenAPI, DB schema, event schema, node-agent protocol, audit-log format, permission model, Engine trait) freeze at the end of M0. After that, a contract change is a standalone PR, never mixed into feature work [D-0036].
- Every PR closes a GitHub issue, and issues plus milestones are the task source of truth [D-0036].
- Every Definition of Done below is a functional check a person can run over SSH on the lab VMs. No performance figures appear here because the lab is nested virtualization and is not valid for benchmarks.

### The lab

The test lab is three nested-virtualization Debian 13 VMs (lab-1, lab-2 and lab-3) on a single physical host, so they share one failure domain; it is not valid for performance benchmarks. Each VM is 2 vCPU / 4 GB RAM / 30 GB on a local disk, with CPU type host and nested KVM. Every agent and contributor tests over SSH.

All three VMs sit on one physical host and therefore share a failure domain. The lab can simulate separate failure domains by stopping VMs or cutting the network between them; the real requirement that a witness sits in a different failure domain than the nodes [D-0017] is documented by the lab, not proven by it.

### Workstreams

After M0 the work splits into workstreams, each in its own crate or package to keep merges small: engine (Cloud Hypervisor, OCI to rootfs, disks and snapshots, networking, console), control-plane (Axum, auth, services, OpenAPI), panel (React against a mock generated from OpenAPI), node-agent (protocol, local reconciliation, self-fencing), ingress (proxy plus Cloudflare Tunnel behind one interface), mcp (rmcp over the same service layer as the API), and installer/releases/docs site (see docs/architecture/architecture.md §17). The later workstreams are the sync/chunk engine and witness/edge (see docs/architecture/architecture.md §17).

### Where HA sits

High availability and backups are not part of the MVP, but the MVP architecture prepares them [D-0033]: the node-agent protocol carries self-fencing from M0 so it is not redesigned later [D-0019], the control plane replicates an append-only numbered hash-chained log from the start [D-0013], and one sync engine serves both node replication and backups [D-0023].

---

## 2. Overview

| Milestone | Name | One-line goal | Depends on |
|---|---|---|---|
| M0 | Contracts | Freeze the contracts and prove a microVM boots from an OCI image | — |
| M1 | Skeleton | One-command install, engine lifecycle, basic panel | M0 |
| M2 | Deploy | Deploy an OCI image with a domain and TLS; nodes survive control-plane loss | M1 |
| M3 | AI-native | Agents act through MCP under scoped grants, everything audited | M2 |
| M4 | Full experience | Git builds, metrics, web terminal, VM console | M3 |
| M5a | Stateless HA and backups | App replicas behind the edge, cold backups to object storage | M4 |
| M5b | Witness, leases and warm replication | Three votes, lease-based fencing, warm volumes, capacity indicator | M5a |
| M6 | Automatic failover and hot | Raft election with a majority, hot policy keeps apps serving | M5b |

---

## 3. Milestones

### M0 — Contracts

#### Scope

- Repository, CI and cargo workspace, plus the OpenAPI spec, DB schema and event schema [D-0033].
- Node-agent protocol, including self-fencing [D-0019]. A lease is a node's right to run a stateful app. `expiring` leases (`warm`/`hot`) end when they are not renewed in time; `local` leases (`none`/`cold`) end only by an explicit revoke or an operator declaring the node lost [D-0047]. When the arbiter is unreachable an `expiring` lease first becomes `uncertain` (keep running, start nothing new) and only then `expired` (self-fence); see docs/contracts/node-agent-protocol.md §8.
- The spike "boot a microVM from an OCI image" [D-0033], on lab-1, using Cloud Hypervisor and KVM [D-0007].
- The bases documents: PRD, ADRs, architecture, contracts, threat model, roadmap, workstream briefs, glossary, OSS files [D-0040].
- The Engine trait, so other backends can be added later without rewriting the control plane [D-0007].

External facts behind this milestone are verified in the research files. Cloud Hypervisor: v54.0, Apache-2.0 AND BSD-3-Clause, an HTTP API over a UNIX socket, snapshots that hold RAM and device state but not disks, virtio-fs that needs an external virtiofsd, text consoles only (serial, virtio-console; no VNC or framebuffer) and a host kernel of 6.8 or later (docs/research/cloud-hypervisor.md); boot under 100 ms remains a Hypothesis. The OCI-to-rootfs conversion and the minimal init are ours: a minimal Rust init and our own OCI-to-ext4 pipeline, with layer paths resolved inside the rootfs (`openat2` with `RESOLVE_IN_ROOT`), zstd layers, and config delivered through a config disk or the kernel command line [D-0061]. utoipa 6 with orval or openapi-ts generates usable client code from the spec, with one known orval break (docs/research/rust-api-and-mcp-tooling.md).

#### Definition of Done

- [ ] On lab-1, a clean clone builds the whole workspace and the CI validation script (OpenAPI, DB schema, event schema, node-agent protocol) exits 0. The same validation runs in CI.
- [ ] Without KVM, two tests pass against an in-process primary and a fake engine: revoking a `local` lease fences its VM, and a test-only short `expiring` lease expires and fences within the bound in node-agent-protocol.md §8.4 (both tests are required by §8.7).
- [ ] The node's lease state machine implements `held` -> `uncertain` -> `expired` and `revoked` as in node-agent-protocol.md §8.4; `uncertain` never stops a stateful app.
- [ ] On lab-1, the spike boots an OCI image converted to a root disk inside a Cloud Hypervisor microVM and the guest's serial console prints the expected marker [D-0033].
- [ ] On lab-1, the contracts directory contains OpenAPI, DB schema, event schema, node-agent protocol, audit-log format, permission model and Engine trait, each listed in the contracts index [D-0040].
- [ ] Every contract element tied to an accepted decision is written as decided; the only remaining open items are listed in the contracts README and none blocks the freeze.

#### Demo script

1. On lab-1, run the workspace build and the contract validation. Expected: exit code 0 with each schema reported as valid.
2. On lab-1, run the OCI spike: convert the image, start the microVM, attach to the serial console. Expected: the guest boots and prints the marker line.
3. Run the lease tests from the node-agent protocol. Expected: both pass — revoking a `local` lease fences its VM, and a test-only short `expiring` lease expires and fences within the bound in node-agent-protocol.md §8.4.
4. Show the contracts index and confirm that contracts freeze at the end of M0. Expected: the maintainer approves the freeze.

#### Dependencies

- Nothing precedes M0. The rootfs pipeline (`mkfs.ext4 -d` plus an OCI registry client) is verified by the M0 spike.
- Q-0003 (rootfs conversion and minimal init) is answered by the M0 spike: our own conversion pipeline and init, validated there [D-0061].
- Realtime transport, lease modes, text-only consoles, the OpenAPI source of truth, provider credentials, cluster keys and the host layout with the one-time setup token are decided [D-0057, D-0047, D-0056, D-0048, D-0051, D-0050, D-0054]. Custody of the cluster keys on promotion stays open and is decided with promotion in M5b [D-0067]; it does not block the freeze.
- Contracts freeze at the end of M0; any later change is a standalone PR [D-0036].

#### Explicitly out of scope

- Any deploy flow: no node agent talking to a control plane, no domain, no TLS. That is M2.
- Any MCP work: agents appear in M3.
- CI running on a self-hosted runner: deferred, because a self-hosted runner would let fork PRs on a public repository run arbitrary code on the project's infrastructure.

---

### M1 — Skeleton

#### Scope

- One-command installer [D-0033].
- Engine lifecycle: create, start, stop and delete a VM [D-0033].
- Basic panel [D-0033].
- Signed release binaries and a reproducible build, because the installer is `curl | sh` and the tool controls infrastructure [D-0027].
- The panel is built against a mock generated from the frozen OpenAPI spec (see docs/architecture/architecture.md §17).

#### Definition of Done

- [x] On a freshly reinstalled lab-1, the one-command install completes and `quoro` answers a version command.
- [x] On lab-1, creating a VM through the API returns an id, starting it makes it appear as running, and stopping then deleting it leaves no VM behind.
- [x] On lab-1, the same create/start/stop/delete sequence works from the panel in a browser, with the state shown in the list after each step.
- [x] On lab-2, the release binary's signature verifies against the published signer identity and the Sigstore trust root [D-0027, D-0081].
- [x] On lab-2, rebuilding from the same source produces a byte-identical artifact to the published one [D-0027].

#### Demo script

1. Reinstall lab-1 from the base image, log in over SSH, run the one-command installer. Expected: it finishes without manual steps and prints the version.
2. Open the panel, create a VM, start it, watch it become running. Expected: the list shows the new state.
3. Stop the VM and delete it. Expected: the list is empty again.
4. On lab-2, verify the signature of the downloaded binary and rebuild from source. Expected: signature valid, hashes match.

#### Dependencies

- M0 contracts, frozen.
- D-0027 shapes the release step.
- Cloud Hypervisor's HTTP API over a UNIX socket is the engine's start/stop path (docs/research/cloud-hypervisor.md).

#### Explicitly out of scope

- Deploying an application image with a domain and TLS: M2.
- Live logs: M2.
- Any HA behaviour: M5a onwards.

---

### M2 — Deploy

#### Scope

- Node agent: it stores desired state locally and reconciles it, dialing out to the control plane and never being dialed into [D-0011].
- Deploy an OCI image with a domain and TLS [D-0033].
- Live logs in the panel, streamed over SSE [D-0033, D-0057].
- Ingress: a reverse proxy with ACME for hosts that have a public IP, or Cloudflare Tunnel for homelabs behind NAT, both behind one interface [D-0024]. The lab has no public IP, so ACME HTTP-01 cannot run there; lab TLS goes through Cloudflare Tunnel, with ACME exercised separately through DNS-01 [D-0055].
- The control plane stays the single writer; on a single node that node is the control-plane primary [D-0012].

The ingress backend is an embedded Rust reverse proxy — hyper, rustls and an ACME client crate — behind the ingress interface that the Cloudflare Tunnel backend also implements; Caddy driven through its admin API stays as a fallback backend if the M2 spike shows ACME is too costly [D-0059]. The maturity of the ACME crate is a research task before M2 (Hypothesis until it is verified) [D-0059]. The node mesh is not needed here: the control-plane session is TLS dial-out (D-0011), and the mesh is first needed in M5a for cross-node replica traffic [D-0063]. Cloudflare Tunnel behaviour: the load balancer sees all replicas of one tunnel as one endpoint and cloudflared replicas have no app health check, so per-node failover needs one tunnel per node behind a load balancer (docs/research/cloudflare-lb-and-tunnel.md).

#### Definition of Done

- [x] Roles: lab-1 runs the control plane (control-plane primary); lab-2 runs a node agent that dials out to lab-1 [D-0011, D-0012].
- [x] On lab-2, deploying an image through the panel yields an app that answers HTTP 200 on its domain.
- [x] On lab-2, the app is served over TLS for its domain through Cloudflare Tunnel with a test domain the maintainer controls [D-0024, D-0055].
- [x] On the lab, the ACME path issues a certificate through DNS-01 against a test zone (separate ingress task) [D-0055].
- [x] On lab-2, the panel's live log view shows new lines within the same session while the app writes to stdout; live logs are streamed over SSE [D-0057].
- [x] Stop the control plane on lab-1; the app on lab-2 keeps answering HTTP 200 and the node agent keeps reporting its desired state locally [D-0011].
- [x] On lab-2, kill the app's process while the control plane on lab-1 is still stopped; the node agent restarts it and the domain answers 200 again [D-0011].

#### Demo script

1. On lab-2, install Quoro and let the node agent connect out to the control plane on lab-1. Expected: the node appears as connected in the panel.
2. Deploy an OCI image with a domain assigned; then open live logs and trigger a request. Expected: the app answers 200 on that domain with a valid certificate, and the request line appears in the log view.
3. Stop the control plane on lab-1. Expected: the app on lab-2 still answers 200.
4. Kill the app process on lab-2. Expected: the node agent restarts it and the domain answers 200 again.
5. Start the control plane again and check the node list. Expected: no duplicate app.

#### Dependencies

- M1 lifecycle and installer.
- An ACME/TLS crate spike before M2 (obtain and renew one certificate).
- D-0011 (dial-out and local reconciliation) and D-0024 (ingress).
- Provider credentials (Cloudflare API and tunnel tokens, DNS zones, S3 keys) are entered and stored through the human-only providers API (`/api/v1/providers`, scope `provider:manage`) with encrypted secrets [D-0051].
- Needed before or during M2: an ACME crate maturity check, with the Caddy admin-API backend as the fallback [D-0059].

#### Explicitly out of scope

- Agents and MCP: M3.
- Several nodes acting as replica nodes of the control plane: the manual promotion of a replica node arrives in M5 (D-0016), and automatic election is M6.
- Git builds: M4.

---

### M3 — AI-native

#### Scope

- MCP server exposing read plus restart and redeploy [D-0033], built on rmcp over the same service layer as the API (see docs/architecture/architecture.md §17).
- Agents as first-class identities, separate from humans, recording who, on whose behalf and why [D-0029].
- Grants with scope, resource selector, TTL and optional maximum uses; incident-scoped grants that auto-expire; just-in-time approval for destructive actions [D-0030].
- Audit log, append-only and hash-chained, written by the same path as the API; destructive actions and refusals included [D-0031].
- Event webhooks per event, so a webhook can wake an agent with incident context [D-0032].
- Documentation stays Markdown in the repository; the docs-site tooling is picked during M3 planning together with the v0.1.0 preview [D-0065].

rmcp 3.8 is the official Rust MCP SDK, with three semver-major releases between March and July 2026, so the MCP surface is pinned to a checked version (docs/research/rust-api-and-mcp-tooling.md).

> **Release callout (D-0064, D-0137).** v0.1.0, the experimental preview planned for the end of this milestone, was tagged after the M4 demo instead and contains M4 too [D-0137].

#### Definition of Done

- [x] On lab-1, tell an agent the app on lab-2 is down; the agent restarts it through MCP, the app answers 200, and the audit log holds an entry naming the agent, the human who authorized it and the reason [D-0033, D-0029].
- [x] On lab-1, an agent holding a read-only grant attempts a restart and is refused; the refusal appears in the audit log [D-0030, D-0031].
- [x] On lab-1, an agent whose grant TTL has passed is refused on the next call, with the reason shown as grant expired [D-0030].
- [x] On lab-1, revoke an agent's grant in the panel; the agent's next call is refused [D-0030].
- [x] On lab-1, an agent attempts a delete without approval; instead of executing, the panel shows a pending approval request, and approving it lets the action run under a short-lived extra grant [D-0030].
- [x] On lab-1, the audit chain verifies from the first entry to the last; after one entry is edited in the local copy, verification reports the exact entry as broken [D-0013, D-0031].
- [x] On lab-1, an event webhook fires on an app-down event and an agent is woken with the incident context [D-0032].
- [x] On lab-1, the reference agent runner in `examples/agent-runner/` receives the wake webhook, verifies its signature, mints an access token with its own agent key and acts through MCP within the incident grant, passing `incident_id` and `justification` [D-0049, D-0032].

#### Demo script

1. On lab-2, stop the app's process and disable the node agent's restart for the demo, so the app stays down; then wake the agent through the app-down webhook. Expected: the agent reads state over MCP and restarts the app.
2. Load the app's domain. Expected: HTTP 200.
3. Open the audit log in the panel. Expected: one entry with agent name, model, owner, on-behalf-of human, triggering event, tool, parameters and result [D-0029, D-0031].
4. Try the same restart with a read-only grant and again with an expired grant. Expected: both refused and both audited.
5. Ask the agent to delete the app. Expected: a pending approval request appears instead of a deletion.
6. Run audit verification, then edit one audit line in a copy and verify again. Expected: clean pass, then a reported break at that entry.

#### Dependencies

- M2 (a working deploy and node agent to operate on).
- D-0029 to D-0032 define the model; D-0013 supplies the replicated log the audit chain rides on.
- Axum SSE and WebSocket routes verified before the M3 event stream.
- The agent runner that receives the wake webhook and starts an agent against Quoro's MCP endpoint is a reference example in `examples/agent-runner/`, outside the quoro binary and owned by the MCP workstream [D-0049]. It verifies `Quoro-Signature` on the delivery, reads the event, `WebhookWake.agent_id` and `incident_id`, holds that agent's own agent key configured by the operator at the runner (Quoro never sends credentials in a webhook), mints a 5-minute access token through `POST /api/v1/auth/agent-token`, and acts through Quoro's MCP endpoint within the incident grant, passing `incident_id` and `justification` on every mutating tool [D-0049].
- Realtime transport is decided: SSE for one-way streams (event stream, live logs, deployment progress) and WebSockets for bidirectional sessions (terminal, VM console) [D-0057].

#### Explicitly out of scope

- Metrics dashboards and the web terminal: M4.
- Off-site copy of the audit log to S3/R2 with object lock [D-0031]: ships in M5a with the S3-compatible path [D-0053].
- Agents approving their own destructive actions: never allowed by design [D-0030].

---

### M4 — Full experience

#### Scope

- Git builds, starting with Dockerfile-only builds [D-0033, D-0060]. A BuildKit daemon runs inside an ephemeral builder microVM (Hypothesis: not yet tried under Cloud Hypervisor) and its output goes to a node-local OCI layout, which then enters the normal OCI-to-rootfs path [D-0060]. Railpack and Nixpacks are re-verified before M4 (Hypothesis: Nixpacks is in maintenance and Railpack is its successor), and a zero-config builder may be added later as a second Builder implementation [D-0060].
- Metrics per app and per node, surfaced in the panel; telemetry and app logs stay on each node, only state is replicated [D-0014].
- Web terminal into the panel [D-0033].
- VM console for a microVM: a text console over the microVM's serial or virtio-console socket, shown with xterm.js (docs/research/cloud-hypervisor.md: no VNC or framebuffer); consoles are text-only and there is no noVNC in the stack [D-0056].
- Decide the curated catalog's format, owner, location and signature scheme at M4 planning, before the first template ships [D-0070].

> **Release callout (D-0064).** M4 complete is the first general release and the broad announcement [D-0064]; the experimental v0.1.0 preview is tagged at the end of M3. This milestone still needs its demo and approval before M5a [D-0036].

#### Definition of Done

- [x] On lab-2, point an app at a Git repository with a Dockerfile; the build runs, produces an image and the app answers 200 on its domain.
- [x] On lab-2, a build that fails shows the failing step and its log output in the panel.
- [x] On lab-1, the panel shows at least CPU and memory for lab-2 and for one app on it, refreshed without a page reload [D-0014].
- [x] On lab-2, open the web terminal, run a command and see its output; the session is recorded in the audit log [D-0031].
- [x] On lab-2, open the VM console of a running microVM and see the guest's boot output on the serial console.

#### Demo script

1. Create an app from a Git repository with a Dockerfile. Expected: the build runs and the app answers 200.
2. Push a commit that breaks the build. Expected: the panel shows the failing step and its log output.
3. Open the metrics view for lab-2 and for the app. Expected: CPU and memory values appear and update.
4. Open the web terminal, run `uname -a` in the app's context, then open the VM console. Expected: the terminal output appears and is in the audit log, and the console shows guest boot or login output.

#### Dependencies

- M3 (audit and grants, so terminal and console access are attributable).
- D-0014 constrains where telemetry lives.
- Standalone contract PRs for the web terminal, VM console and metrics endpoints, merged before the feature work (D-0036).
- Needed before or during M4: Dockerfile-only builds, with Railpack and Nixpacks re-verified before M4 [D-0060].
- This milestone is the first general release [D-0064].

#### Explicitly out of scope

- App replicas and edge load balancing: M5a.
- Any backup or restore UI: M5a.
- A docs site: documentation stays Markdown in the repo until the first public release, and the docs-site tooling is picked during M3 planning together with the v0.1.0 preview (Q-0009, D-0065). It belongs to the installer/releases/docs-site workstream, not to a product milestone (see docs/architecture/architecture.md §17).

---

### M5a — Stateless HA and backups

#### Scope

- Stateless app replicas: extra running instances of a stateless app, placed on several nodes and put behind the edge load balancer, so the edge stops sending traffic to a dead node [D-0020].
- Edge load balancing, with the edge as its own role of the same binary and able to combine with witness [D-0012, D-0018].
- Backups to S3 or R2, at the `cold` policy: backups only [D-0020, D-0023].
- The content-addressed chunk engine, used for backups first [D-0023].
- Backups of the control-plane SQLite database and of catalog templates go through that same chunk engine with client-side encryption, taken from consistent snapshots (the SQLite backup API or `VACUUM INTO`), not through Litestream [D-0069].
- HA policies are named `none`, `cold`, `warm` and `hot`, and the panel shows what each costs [D-0020].
- Local, non-replicated persistent volumes: create, attach to an app, survive redeploy [D-0052].
- Off-site copy of the audit log to an S3-compatible bucket with object lock [D-0031, D-0053].
- Vulnerability tracking (ADR 0138): Quoro lists the components it ships (guest kernel, Cloud Hypervisor, its own crates) and checks them daily against OSV; each app image gets a package inventory on its node, matched on the primary, shown per app in the panel; a critical finding opens an `app.vulnerable` incident that can wake an agent; Git apps can rebuild on a refreshed base, optionally on their own. Host kernels stay the operator's: Quoro reports them and never upgrades them. A short spike comes first.

Cloudflare Load Balancing is a paid add-on from $5/month; an HTTPS monitor on the app hostname tests the app; the load balancer sees all replicas of one tunnel as one endpoint; cloudflared replicas have no documented app health check (Hypothesis); per-node failover needs one tunnel per node behind a load balancer (docs/research/cloudflare-lb-and-tunnel.md).

D-0016 places manual promotion of a replica node to primary in M5, and it ships in M5b alongside leases and fencing [D-0016, D-0067]. Promotion is manual: a command or a button, or an agent doing it with human approval [D-0016].

#### Definition of Done

- [x] With lab-1 and lab-2 as nodes and lab-3 as the edge, a stateless app runs two app replicas, one on each node, and the edge sends traffic to both.
- [x] Power off lab-1; the app still answers 200 on its domain through the edge on lab-3, served by the app replica on lab-2 [D-0020]. (In the demo, the outage was simulated by stopping lab-1's node agent and killing the replica's VM process.)
- [x] On lab-1, a volume is created and attached to an app; data written to it survives a redeploy [D-0052].
- [x] With lab-1 back, take a `cold` backup of a stateful app running on lab-1 to an S3-compatible bucket, then restore it onto lab-2 and confirm the restored app serves the data that was present at backup time [D-0020, D-0023].
- [x] On lab-1, the panel shows the `cold` policy's cost for the app, and switching the policy changes the shown cost [D-0020].
- [x] On lab-3, an edge health check removes lab-1 from rotation after lab-1 is powered off, and adds it back after lab-1 returns. (Simulated the same way.)
- [x] On lab-1, one audit segment is exported with object lock to a bucket; an overwrite or delete of it is refused; truncating the tail of the local audit log is detected against the exported copy [D-0031, D-0053].

#### Demo script

1. Set up lab-1 and lab-2 as nodes and lab-3 as edge.
2. Deploy a stateless app with two app replicas and request its domain several times. Expected: the panel lists both replicas on different nodes, and responses come from both (each instance identifies itself).
3. Power off lab-1. Expected: the domain keeps answering 200, requests land on lab-2 only. (In the demo, the outage was simulated by stopping lab-1's node agent and killing the replica's VM process.)
4. Power lab-1 back on and wait for the node to reconnect. Expected: the edge sends traffic to it again. (In the demo, the node agent was started again.)
5. Take a `cold` backup of a stateful app to the bucket, then restore it on lab-2. Expected: the restored app serves the backed-up data.
6. Create a volume, attach it to an app and write a file to it; redeploy the app. Expected: the file is still there after the redeploy [D-0052].
7. Export one audit segment with object lock to the bucket, then try to overwrite or delete it. Expected: both are refused; truncating the tail of the local audit log is detected against the exported copy [D-0031, D-0053].

#### Dependencies

- M4.
- D-0020 (policies), D-0023 (chunk engine), D-0018 (edge as voter, relevant only from M5b), D-0012 (roles in one binary).
- Briefs for the sync/chunk engine and the witness/edge workstreams are written and approved before M5a starts.
- Local, non-replicated volumes ship in M5a [D-0052]; a standalone post-freeze contract PR adds the volume endpoints before M5a.
- A standalone contract PR adds the HA policy cost and N+1 capacity fields.
- The node mesh is kernel WireGuard configured by Quoro over netlink (no wg-quick), with a per-node keypair and peers plus allowed IPs in the desired state; boringtun in userspace is only a fallback and our own implementation is rejected [D-0063]. It is first needed in M5a for cross-node replica traffic; the control-plane session is TLS dial-out (D-0011), so M2 needs no mesh, and a publicly reachable peer acts as the relay hub for NAT [D-0063].
- The snapshot primitive the chunk engine reads from: reflink snapshots plus a full content-defined chunk scan for the M5a `cold` backups [D-0062].
- The off-site audit-log copy [D-0031] ships in M5a with the S3-compatible path [D-0053].
- Object lock in compliance mode must be verified per provider (R2, S3, B2, MinIO) before M5a (research task).
- Object storage choice: S3-compatible; R2 offers an atomic conditional PUT at 1 write/s per key with no TTL, and is only needed as a lease arbiter from M5b (docs/research/r2-conditional-writes.md).

#### Explicitly out of scope

- `warm` and `hot`: M5b and M6.
- Witness, leases and fencing for stateful apps: M5b.
- Automatic failover of the control plane: M6.
- Distributed storage such as Ceph [D-0034].

---

### M5b — Witness, leases and warm replication

#### Scope

- Witness role: a lightweight daemon that only breaks ties and stores no data, in a different failure domain than the nodes [D-0017].
- Quoro reports that HA needs a third vote when only two nodes exist, and offers a witness [D-0017].
- Leases and lease-based fencing [D-0019]. A lease is a node's right to run a stateful app. `expiring` leases (`warm`/`hot`) end when they are not renewed in time; `local` leases (`none`/`cold`) end only by an explicit revoke or an operator declaring the node lost [D-0047]. When the arbiter is unreachable an `expiring` lease first becomes `uncertain` (keep running, start nothing new) and only then `expired` (self-fence); see docs/contracts/node-agent-protocol.md §8.
- Replicated volumes at the `warm` policy: data continuously replicated, image pulled, instance stopped [D-0020], shipped by the content-addressed chunk engine [D-0023].
- The N+1 capacity indicator: "if node A dies, do its apps fit on the others?" [D-0021].
- Failback behaviour: a returning node rejoins fenced, starts nothing stateful, quarantines its divergent writes as a snapshot recorded in the audit log, resyncs incrementally, and by default no failback happens [D-0022].
- A planned switchover is available when the user prefers the original node: wait for lag near zero, wait for a low-traffic window, freeze writes, final sync, move the lease, shift traffic, drain connections; it loses no data [D-0022].
- The edge can also be the third voter, as the witness+edge combination of the same binary [D-0018].

External arbiters are optional and their behaviour is verified. R2 conditional writes as a lease arbiter: an atomic conditional PUT, 1 write/s per key, no TTL, and fencing tokens are still required (docs/research/r2-conditional-writes.md). Cloudflare Durable Objects as the arbiter: fits on the Workers Free plan, needs a Cloudflare account and a Worker, and fencing tokens are still required (docs/research/durable-objects.md). The snapshot primitive under the chunk engine is reflink snapshots, natively or in a node-managed XFS volume pool, plus a full content-defined chunk scan for the M5a `cold` backups [D-0156]; `warm` keeps the same snapshots and finds the ranges that changed between two of them by comparing their extent maps (FIEMAP), with no lvm-thin and no btrfs [D-0172].

Failure domains: all three lab VMs run on one physical host. The lab therefore simulates separate failure domains by stopping VMs or cutting the network between them. The real requirement that the witness sits in a different failure domain [D-0017] is documented here, not proven by the lab.

#### Definition of Done

- [ ] Roles: lab-2 is the control-plane primary and a node; lab-1 is a node hosting the stateful app; lab-3 is added as witness during the test [D-0012, D-0017].
- [ ] With only lab-1 and lab-2 as nodes, Quoro reports that HA needs a third vote and offers to add a witness [D-0017].
- [ ] Add lab-3 as witness; Quoro then reports three votes, and lab-3 stores no application data.
- [ ] A stateful app runs at `warm` on lab-1 with its data shipped to lab-2; the panel shows the last shipped snapshot on lab-2.
- [ ] Cut lab-1 off from lab-2 and lab-3; lab-1 self-fences its stateful apps and they stop on lab-1 [D-0019].
- [ ] While lab-1 still holds the app's lease, Quoro refuses to start the warm copy on lab-2 [D-0019].
- [ ] After lab-1's lease is lost, an operator (or an agent with human approval) starts the warm copy on lab-2, and it contains the data of the last shipped snapshot [D-0016, D-0020, D-0023]. Automatic per-app failover is M6.
- [ ] Bring lab-1 back; it starts nothing stateful, its copy of the app becomes the `warm` copy following lab-2 (lab-2 is now the lease holder and source of truth), and its divergent writes appear as a quarantined snapshot recorded in the audit log [D-0022].
- [ ] Write a known record to the app, then run a planned switchover back to lab-1; it completes, the app serves from lab-1, and the record is present [D-0022].
- [ ] The N+1 indicator answers "if lab-1 dies, do its apps fit?" with yes, or with no plus the missing resources [D-0021].
- [ ] An operator promotes a replica node to primary; the promotion is refused while the old primary may still hold expiring leases unless the operator acknowledges the fence [D-0067].
- [ ] Before expiring leases ship, the concurrent-fence and mid-fence crash tests pass [D-0075].
- [ ] A held expiring lease survives an agent restart within the same host boot [D-0072].

#### Demo script

1. Start with lab-1 and lab-2 as nodes only and open the HA view. Expected: Quoro says a third vote is missing and offers a witness [D-0017]. Add lab-3 as witness. Expected: Quoro reports three votes and lab-3 shows no volumes.
2. Deploy a stateful app at `warm` on lab-1. Expected: the panel shows the data being shipped and lab-2 holding the last shipped snapshot.
3. Cut the network between lab-1 and the other two. Expected: lab-1's app stops itself (self-fence). Try to start the warm copy on lab-2 before the lease is lost: refused. After the lease is lost, start it: it serves the last shipped data.
4. Reconnect lab-1. Expected: it starts nothing stateful, its copy becomes the warm copy following lab-2, and a quarantined snapshot of its divergent writes is in the audit log [D-0022].
5. Start a planned switchover to lab-1 in the panel, then open the N+1 indicator and ask "if lab-1 dies?". Expected: the switchover completes with no data loss and the app serves from lab-1; the indicator answers yes, or no plus the missing resources [D-0021].

#### Dependencies

- M5a (edge, app replicas, chunk engine, bucket).
- D-0017, D-0018, D-0019, D-0020, D-0021, D-0022, D-0023.
- A standalone contract PR adds the HA policy cost and N+1 capacity fields.
- Needed before or during M5b: a lab measurement of how precise and correct the FIEMAP comparison of two reflink snapshots is on the XFS volume pool, which also sets the default replication interval of `warm` [D-0172].
- This milestone does not deliver automatic promotion of the control plane; that is M6 [D-0016].

#### Explicitly out of scope

- Automatic control-plane election: M6.
- The `hot` policy: M6.
- Automatic failback: never, it is always planned and approved [D-0022].
- Automatic per-app failover: M6 [D-0033].

---

### M6 — Automatic failover and hot

#### Scope

- Automatic election of the control-plane primary with a majority, using Raft semantics; automatic failover only with at least three votes [D-0016].
- The `hot` policy: the instance runs on another node and replicates, so a node loss is covered by a running copy [D-0020].
- Per-app failover rides on the same lease mechanism as the election [D-0019].
- Failback remains never automatic [D-0022].

All three lab VMs are on one physical host. The lab simulates a split by cutting the network between VMs and a node loss by stopping a VM. The requirement that voters sit in different failure domains [D-0017] is documented, not proven, by the lab.

#### Definition of Done

- [ ] With lab-1, lab-2 and lab-3 as voters, powering off the primary node triggers an automatic election and a new primary takes writes without any human action [D-0016].
- [ ] Cut the network into a minority partition; the minority side never acts as primary, and no node in it accepts writes [D-0016].
- [ ] In the majority partition, writes still succeed and the audit chain still verifies after the partition heals [D-0013, D-0031].
- [ ] A `hot` app keeps answering 200 on its domain across the loss of the node that was serving it, with a running copy on another node [D-0020].
- [ ] Bring the failed node back; it returns as a replica node and no automatic failback happens; a planned switchover is the only way to move the workload back [D-0022].

#### Demo script

1. Run three voters: lab-1, lab-2, lab-3. Note the current primary.
2. Power off the primary. Expected: a new primary is elected automatically, the panel shows it without a manual step, and a change made through the panel succeeds on it.
3. Cut the network so one node is alone. Expected: that node refuses writes and does not claim to be primary.
4. Deploy a `hot` app and stop the node serving it. Expected: the domain keeps answering 200 from the running copy. Power the stopped node back on. Expected: it rejoins as a replica node, nothing moves automatically, and a planned switchover is available in the panel [D-0022].

#### Dependencies

- M5b (witness, leases, replication).
- D-0016, D-0019, D-0020, D-0022.
- Needed before or during M6: snapshot consistency of `hot` volumes with the chosen primitive.

#### Explicitly out of scope

- Managed hosting: Later.
- Synchronous replication as the default: async is the default, synchronous is optional for nearby nodes [D-0020].

---

## 4. Later

- An optional managed offering may come later [D-0003]; nothing in this roadmap depends on it. The self-hosted witness is free and first-class [D-0017].
- A container-fallback Engine implementation is a later backend behind the Engine trait, because many VPS do not expose nested virtualization [D-0009]. It is not part of the MVP.
- A self-hosted GitHub Actions runner stays deferred, because a self-hosted runner would let fork PRs on a public repository run arbitrary code on the project's infrastructure.

---

## 5. Out of scope for now

The following are explicitly excluded, because each could eat the project [D-0034]:

- Live migration [D-0034].
- Distributed storage such as Ceph [D-0034].
- Complex networking: SDN and VLANs [D-0034].
- Multi-tenant operation with billing [D-0034].
- Writing our own VMM; Quoro depends on KVM and Cloud Hypervisor instead [D-0007, D-0034].

---

## 6. Open questions affecting the roadmap

Open questions are open. This table lists them with options where the ledger gives options, and never picks one.

| Q-id | Question | Needed by milestone |
|---|---|---|
| Q-0008 | Domain and trademark check | before the M3 v0.1.0 tag (D-0064) |

Decided and no longer open: the ingress proxy [D-0059], the Git build method [D-0060], the OCI-to-rootfs conversion and init [D-0061], the snapshot primitive [D-0062], the node mesh [D-0063], the first public release [D-0064], manual promotion of a replica node [D-0067], docs-site timing [D-0065], encrypted SQLite and template backups [D-0069] and when the curated catalog is decided [D-0070].

### Answered verification questions

| Q-id | Topic | Answer |
|---|---|---|
| Q-0010 | Cloud Hypervisor capabilities | v54.0, Apache-2.0 AND BSD-3-Clause, HTTP API over a UNIX socket; snapshots hold RAM and device state, not disks; virtio-fs needs an external virtiofsd; text consoles only (serial, virtio-console), no VNC or framebuffer; host kernel 6.8 or later. Boot under 100 ms remains a Hypothesis. docs/research/cloud-hypervisor.md |
| Q-0011 | R2 conditional writes as a lease arbiter | Atomic conditional PUT, 1 write/s per key, no TTL; fencing tokens still required; B2 appears to lack it (Hypothesis). docs/research/r2-conditional-writes.md |
| Q-0012 | Durable Objects as an external lease arbiter | Fits as arbiter on the Workers Free plan; needs a Cloudflare account and a Worker; fencing tokens still required. docs/research/durable-objects.md |
| Q-0013 | Cloudflare Load Balancing and cloudflared failover | LB is a paid add-on from $5/month; an HTTPS monitor on the app hostname tests the app; the LB sees all replicas of one tunnel as one endpoint; cloudflared replicas have no documented app health check (Hypothesis); per-node failover needs one tunnel per node behind an LB. docs/research/cloudflare-lb-and-tunnel.md |
| Q-0014 | rmcp, Litestream and utoipa/orval maturity | rmcp 3.8 official, three semver-major releases between March and July 2026; Litestream 0.5 has no client-side encryption (Q-0026); utoipa 6 with orval/openapi-ts works, one known orval break. docs/research/rust-api-and-mcp-tooling.md |
| Q-0015 | VPS providers exposing KVM and nested virtualization | Nested KVM is the exception on cheap VPS; bare metal is the reliable path; probe /dev/kvm with a real boot. docs/research/vps-nested-virt.md |

Every "to verify" claim is researched before it enters a document; the answered ones above cite their research file, and unconfirmed ones stay marked as hypotheses [D-0040].
