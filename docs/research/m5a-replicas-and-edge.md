# M5a spike: stateless replicas across nodes, the edge role and the node mesh

- **Question:** how do extra running instances of a stateless app on several nodes sit behind an edge that balances load, checks health and fails over (roadmap M5a, ADR 0012, ADR 0018, ADR 0020)? How does the edge combine with the witness (ADR 0018), the ingress (ADR 0024, ADR 0059) and the WireGuard mesh (ADR 0063)? What can the three-VM lab show?
- **Researched:** 2026-11-04. Desk research against `main`, plus read-only facts from lab-1 and lab-2 (kernel config, filesystems, loaded services). **No prototype ran.** Everything under "Design" is a recommendation and is marked **Hypothesis** where it rests on untried behaviour.
- **Status:** design proposal for the M5a contract PR; the choices are raised as proposals for the maintainer.

## Short answer

1. **The edge is Quoro's own proxy, not a provider load balancer.** A machine with the `edge` role enrols like a node, receives a signed desired state with routes whose backends are app instances on other nodes, and runs the embedded Rust proxy that `crates/ingress` already ships (ADR 0059). It checks every backend itself and keeps serving the last route table when the primary is gone (ADR 0011). Cloudflare Load Balancing stays an optional `LoadBalancerProvider` for later: it is paid, and it cannot see an app replica behind a shared tunnel (`cloudflare-lb-and-tunnel.md`).
2. **Replicas are a count on the app, spread one per node.** `replicas: N` (default 1) applies to stateless apps only. The scheduler places at most one instance per node, so a node loss never takes more than one replica. Stateful apps stay at one instance until the `hot` policy (M6).
3. **The edge reaches instances over kernel WireGuard, in a star.** In M5a only edge-to-instance traffic crosses nodes, so the mesh can be a star with the edge as hub. Node-to-node peers are added in M5b for `warm` replication. Each node needs its own guest subnet first (issue #77): today every node gets the same `10.42.0.0/24`, so an edge could not route to two nodes' instances.
4. **Lab:** both lab VMs that are up have the WireGuard module (`CONFIG_WIREGUARD=m`), so the mesh needs no install. lab-3 (the edge in the roadmap DoD) is down and has 2 GB, which is enough for an edge that runs no microVM. The DoD powers off lab-1, which today runs the control plane, so either the primary moves to lab-2 for the demo or nodes must follow a primary whose address changes (issue #157) first. See [What the lab can show](#what-the-lab-can-show).

## Findings

### 1. What exists on `main`

| Area | State on `main` (2026-11-04) | Label |
| --- | --- | --- |
| Roles | `nodes.roles` is an array of `node`, `witness`, `edge` (`db-schema.sql`); enrolment and the API accept the values; nothing acts on `edge` | Confirmed |
| App instances | One instance per app in M1-M4 (`openapi.yaml`, `App`); `placement.node_ids` constrains the node | Confirmed |
| Routes | `desired_state.spec.routes[].vm_ids` is already a list "one per app replica" (`node-agent-protocol.md` 5.11); the proxy's `Route.backends` is a list and skips a backend that refuses the connection (`crates/ingress/src/route.rs`) | Confirmed |
| Guest network | One `NodeNetwork { subnet, gateway }` per control plane, sent to every node in `spec.network` (`crates/control-plane/src/lib.rs`); issue #77 records that every node gets the same subnet | Confirmed |
| Mesh | Not implemented. ADR 0063: kernel WireGuard over netlink, no `wg-quick`, per-node keypair, peers and allowed IPs in desired state, boringtun only as a fallback | Confirmed |
| Tunnels | One Cloudflare Tunnel per node; the primary writes its ingress rules and DNS, the node gets only a run token (ADR 0094) | Confirmed |
| App health | The node agent checks `health_check.path` locally and reports `app.down` / `app.degraded` | Confirmed |

### 2. Lab facts (read-only, 2026-11-04)

| Fact | lab-1 | lab-2 |
| --- | --- | --- |
| OS and kernel | Debian 13, 6.12.115 cloud kernel | Debian 13, 6.12.113 cloud kernel |
| Memory reported | 2 GB | 4 GB |
| WireGuard | `CONFIG_WIREGUARD=m`, `wireguard.ko.xz` present, not loaded | same |
| Root filesystem | ext4 | ext4 |
| Quoro services | `quoro.service` (control plane) and `quoro-node.service` | `quoro-node.service` only |

lab-3 was not reachable. The lab VMs sit behind the same home router on one physical host, so none of them has a public address. TLS for the demo domain therefore comes from a Cloudflare Tunnel (ADR 0055, ADR 0097).

### 3. Provider load balancing versus an own edge

| | Quoro edge (embedded proxy on an `edge` machine) | Cloudflare Load Balancing with one tunnel per node |
| --- | --- | --- |
| Cost | A small machine | Paid add-on from $5/month (`cloudflare-lb-and-tunnel.md`) |
| Health view | Probes each instance by its address, so it sees each replica | Sees one endpoint per tunnel; an HTTPS monitor on the hostname tests the app through the tunnel (`cloudflare-lb-and-tunnel.md`) |
| Works with the primary down | Yes: it serves its last applied table and keeps checking (ADR 0011) | Yes: the provider keeps running |
| Self-hosted | Yes | No, the traffic path is the provider's |
| Fits ADR 0018 | It is the edge that can later also be the witness | It is the alternative ADR 0018 names for traffic, with a small machine kept only as witness |
| Single point of failure | One edge is a traffic SPOF (ADR 0018) | The provider |

The roadmap DoD names lab-3 as the edge and asks for "an edge health check" on lab-3. Only the own edge does that. The provider path is a later `LoadBalancerProvider` implementation (architecture §11). Its main use is a second, redundant traffic path in front of two edges.

## Design (Hypothesis until built and tested on the lab)

### 4. Replicas

- **API.** `App.replicas`, an integer from 1 to the number of eligible nodes, default 1. It is set on create and changed on update. A change is an ordinary state change that produces a new desired state per node, with no new deployment. A deployment rolls instances one node at a time, and a replica counts as ready once its local health check passes.
- **Placement.** One instance per node: anti-affinity by node, so a node loss costs one replica at most. `placement.node_ids` narrows the candidate set. If fewer eligible nodes exist than `replicas`, the request is refused with `validation_failed`, never silently under-placed. A node that is `disconnected` keeps its instance in the plan. The edge drops it from rotation by health, not by a state change, so nothing reschedules automatically before M6 (ADR 0016).
- **Stateless only.** An app with a volume (stateful) is refused for `replicas > 1` (`conflict`). ADR 0020 keeps more than one running copy of stateful data for `hot` (M6).
- **Identity in responses.** Each instance gets `QUORO_INSTANCE_ID` and `QUORO_NODE_ID` in its environment, so the demo app can name the instance that answered (demo step 2). This is additive and harmless for other apps.

### 5. The edge role

- **Enrolment.** The same binary and join flow as a node, with `roles: ["edge"]`. An edge-only machine runs no engine and needs no KVM: the node agent skips `probe_host` failures for an edge-only role and reports `kvm.available: false` as normal. A `node`+`edge` machine runs both.
- **Desired state.** A new optional `spec.edge`, alongside `spec.routes` (which stays the node's local routes):

  | Field | Meaning |
  | --- | --- |
  | `routes[]` | `hostname`, `domain_id`, `app_id`, `tls_mode`, `health_check` (the app's), and `backends[]` of `{vm_id, node_id, address, port}` where `address` is the instance's guest address in its node's subnet |
  | `tunnel` | The edge's own tunnel when the edge is behind NAT (ADR 0094 shape, one tunnel for the edge) |

- **Health.** The edge probes each backend's `health_check.path` over the mesh every `interval_s`. It removes a backend after `failures` consecutive misses and adds it back after the same number of consecutive passes. It also takes a backend out immediately when a connection is refused or reset (passive), which the proxy already does per request. These are edge-local decisions: they need no primary and change no replicated state. The edge reports backend health in its `status_report`, and the primary can show it and raise `app.degraded` when a replica is out of rotation.
- **TLS and DNS.** A domain on an app with `replicas > 1` is served by the edge: its DNS record or tunnel ingress rule points at the edge instead of a node. On the lab the edge runs the tunnel. On a public-IP edge it runs ACME, as a public node does today (ADR 0024). The nodes keep serving their local routes, so an app with one replica and no edge behaves as in M4.
- **Load balancing.** Round robin over healthy backends, with no stickiness in M5a. A request that fails to connect is retried once on another backend, and only for idempotent methods (GET, HEAD), so a POST is never sent twice.
- **Primary down.** The edge keeps its last verified desired state on disk, exactly as a node does (ADR 0011), and keeps probing. This is what makes the DoD step "power off lab-1" pass whichever lab hosts the primary.
- **Witness later.** ADR 0018 lets the edge be the third voter. Nothing in M5a's edge stores data, so adding the witness role in M5b means adding the vote to the same daemon, not moving the edge.

### 6. The mesh

- **Topology in M5a: a star with the edge as hub.** The only cross-node flow in M5a is edge to instance. A full node-to-node mesh is needed in M5b for `warm` replication (ADR 0063) and can then be added by putting more peers in `spec.mesh`, with no protocol change.
- **Keys.** Each node and edge generates its WireGuard key pair locally and sends only the public key in `hello`. The private key never leaves the machine, matching the node credential (ADR 0063).
- **Desired state.** `spec.mesh`: `listen_port` (default 51820/udp), the machine's mesh address, and `peers[]` of `{node_id, public_key, endpoint?, allowed_ips[], persistent_keepalive_s?}`. A node's `allowed_ips` on the edge are its guest subnet plus its mesh address. A node behind NAT has no `endpoint`: it dials the hub with a keepalive, and WireGuard's roaming updates the hub's view of its address (wireguard.com, "Built-in Roaming"). This also covers lab-1's address changing after a reboot.
- **Netlink, no shell-out.** Two crates fit the license policy (ADR 0080): `netlink-packet-wireguard` 0.5.1 (MIT) with `genetlink` 0.3.1 (MIT), from the same family as `rtnetlink` 0.24.0 (MIT); or `defguard_wireguard_rs` 0.12.5 (Apache-2.0), a higher-level wrapper. `wireguard-control` 2.0.0 is **LGPL-2.1-or-later**, which is not on the ADR 0080 allowlist, so it is out. Recommendation: the netlink-packet crates, because they add no userspace fallback code the project does not use (crates.io metadata, 2026-11-04).
- **Firewall.** On a node, traffic arriving on the WireGuard interface may reach only the guest addresses and ports named in the edge's backend list, and only from the edge's mesh address. Everything else from the mesh is dropped. VM-to-VM isolation (ADR 0084) is unchanged: the mesh carries no guest-to-guest traffic in M5a.
- **Per-node subnets (issue #77) first.** Proposal: one cluster range, `10.42.0.0/16`, with one `/24` per node assigned at enrolment and stored on the node record (`nodes.subnet`). It is never reused while a VM from it is recorded (architecture §9). The upgrade path is that the first node keeps the subnet it has. Every other node gets a new `/24`, and its VMs move to it on their next deploy, which is the same "redeploy to apply" rule as ADR 0136. **Hypothesis:** the node agent can renumber the bridge while no VM runs on it; it cannot do so under running VMs.

## What the lab can show

| Plan | Roles | What it proves | What it does not |
| --- | --- | --- | --- |
| A (the roadmap DoD) | lab-2: primary + node; lab-1: node; lab-3: edge only | Every DoD line: two replicas on two nodes, power off lab-1, the edge on lab-3 removes it and serves from lab-2, then adds it back | Separate failure domains: all three VMs share one physical host |
| B (lab-3 stays down) | lab-2: primary + node + edge; lab-1: node | The same failover behaviour, with the edge co-located on a node (architecture §3 allows `node`+`edge`) | That an edge-only machine works without KVM |

The primary should sit on lab-2 for the demo, not on lab-1, for two reasons. The DoD powers off lab-1. And lab-1's address can change on reboot, so other nodes would lose their session until issue #157 ships. The edge itself does not depend on either point (section 5), but the backup half of the demo does, because it needs the control plane. lab-3 is the right edge because an edge runs no microVM and fits in 2 GB. Bringing it back is the maintainer's call.

## Open risks and unknowns

- **Subnet renumbering of existing nodes** needs a redeploy of their VMs. It must be documented in the release notes, as the M4 terminal change was.
- **NAT.** Two nodes behind different NATs cannot reach each other (ADR 0063). In M5a only the edge needs to be reachable, and on the lab every VM is on one LAN. A real homelab edge needs one forwarded UDP port or a public address.
- **Edge as SPOF.** One edge is a traffic single point of failure (ADR 0018). Two edges with DNS failover, or a provider load balancer in front, are later work.
- **Health-check semantics.** The edge and the node both probe the same path. A slow app sees twice the probe traffic, which is small at default intervals.
- **Not exercised:** WireGuard over nested virtualization (expected to work, since it is plain UDP), the proxy's behaviour under a backend that accepts and hangs (a connect timeout and a request timeout are needed), and HTTP/2 or WebSocket upgrades through the edge.

## Sources

- Roadmap M5a scope and DoD: `docs/product/roadmap.md`
- Roles, networking, ingress and providers: `docs/architecture/architecture.md` §3, §9, §10, §11
- Desired state, routes and tunnels: `docs/contracts/node-agent-protocol.md` §5.11
- Route table code: `crates/ingress/src/route.rs`; node network: `crates/control-plane/src/lib.rs`
- Cloudflare Load Balancing and tunnel replicas: `docs/research/cloudflare-lb-and-tunnel.md`
- WireGuard roaming and key model: https://www.wireguard.com/ (accessed 2026-11-04)
- `netlink-packet-wireguard`: https://crates.io/crates/netlink-packet-wireguard (accessed 2026-11-04)
- `genetlink`: https://crates.io/crates/genetlink (accessed 2026-11-04)
- `rtnetlink`: https://crates.io/crates/rtnetlink (accessed 2026-11-04)
- `defguard_wireguard_rs`: https://crates.io/crates/defguard_wireguard_rs (accessed 2026-11-04)
- `wireguard-control` (LGPL-2.1-or-later): https://crates.io/crates/wireguard-control (accessed 2026-11-04)
- License allowlist: `deny.toml` (ADR 0080)
