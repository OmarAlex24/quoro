# ADR 0266: Where the primary takes a voter's mesh endpoint from (node-to-node peers, section 6.9)

- **Status:** Accepted; refined by [ADR 0296](0296-spec-mesh-on-the-edge-the-warm-target-s-allow-entry-and-no.md)
- **Date:** 2026-11-07
- **Ledger id:** D-0266
- **Tags:** m5b, mesh, contracts, warm, voters
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0296](0296-spec-mesh-on-the-edge-the-warm-target-s-allow-entry-and-no.md)

## Context

Section 6.9 makes every two voters mesh peers and sends an endpoint only to machines that are not behind NAT, but nothing says where the primary gets a voter's endpoint or how it knows it is behind NAT; without a rule no two nodes get a peer and warm replication cannot run

## Decision

Where the primary takes a voter's mesh endpoint from (node-to-node peers, section 6.9).

Gap. node-agent-protocol.md section 6.9 (line 1725, "NAT") says a machine behind NAT has no `endpoint` in the other side's peer entry, and that the primary gives no peer entry to two voters that are both behind NAT. Section 5.11 (`spec.mesh.peers[].endpoint`) says the same. But nothing says where the primary gets a voter's endpoint, or how it knows that a voter is behind NAT:
- `hello` (section 5.1) carries no address of the machine. Only `control_plane_urls` exists, which names the control-plane listener and not WireGuard.
- `nodes.address` and `nodes.behind_nat` exist (openapi.yaml `Node`), but every machine enrolls with `address = NULL` and `behind_nat = 1`, and no operation changes them. Read literally, every node is behind NAT, so no two nodes ever get a peer entry. That would leave warm replication (ADR 0173) and promotion votes (ADR 0196) without a path.
- In M5a only the edge has an endpoint: the source address of its session on `listen_port`, or the first non-loopback IPv4 host of `welcome.primary_urls` when that address is loopback (ADR 0144). Nodes have none, because every node may be behind NAT.

Options:
A (recommended). A voter's endpoint is the source address of its session on `listen_port`, with the same loopback fallback as the edge's (ADR 0144). A voter whose session the primary has not seen since it started has no endpoint. Two voters without an endpoint get no peer entry for each other. Every peer entry between two voters carries `persistent_keepalive_s` 25.
- Costs: a voter that is really behind NAT gets the NAT's public address as its endpoint. Handshakes sent there fail, which is harmless: WireGuard follows the voter's real address once that voter's keepalive handshake arrives, as long as the other side is reachable. Two voters that are both really behind NAT get entries for each other that never connect. That is the limitation sections 6.9 and 8.9 already state, through a different mechanism: the entries exist but carry no traffic. The primary cannot tell this case apart, so it may pair them; `App.warm` then shows the copy `failed` or `syncing`.
- Contract text only. No wire or schema change. It works where all machines share one LAN.
- Sentence A adds to section 6.9 "NAT", after its first sentence: "The primary takes a voter's `endpoint` from the source address of its session on `listen_port`, as for the edge in M5a (ADR 0144); a voter whose session it has not seen since it started has no endpoint, and the primary does not detect NAT, so a voter behind NAT gets its NAT's address and is reached once its own keepalive arrives."
B. `hello` gains an optional `mesh_endpoint` (`host:port`), set by the operator in the node agent's configuration. Absent means behind NAT. This is precise and gives two voters behind NAT no entries for each other, as written today. Costs: a new `hello` field, node and installer configuration, and every reachable voter must be configured, or else it counts as behind NAT.
C. An operation that sets `nodes.address` and `behind_nat` for each machine (openapi.yaml). Costs: a new API surface and operator work for every node. Nothing fills the columns automatically.

Firewall, unchanged by A, B or C: section 6.9 "Firewall" (line 1724) already says "From M5b the target of a warm volume also admits the replication listener: TCP to its own mesh address on the warm entry's port, from the source's mesh address, through the `allow` entry the primary sends it (section 6.13)". Today the node agent turns `allow` entries into prerouting marks, but its `input` chain on `quoro-wg0` accepts only `established,related`, so the target would drop a replication stream even with the entry. The node agent's implementation adds `iifname "quoro-wg0" meta mark <MESH_MARK> accept` to the input chain, before its drop. That is an implementation of the existing text, not a contract change.

Option A was chosen: a contract change of text only adds the sentence above to section 6.9, before the node agent's firewall code.

_Some wording was changed for the public repository._

## Consequences

- [ADR 0296](0296-spec-mesh-on-the-edge-the-warm-target-s-allow-entry-and-no.md) refines which peer entries carry an endpoint: on a machine whose only role is edge, the entries for nodes carry no endpoint and no keepalive, as in M5a. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0266 in the project decision ledger. Do not edit by hand; changes go through a new decision.
