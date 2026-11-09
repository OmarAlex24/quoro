# ADR 0296: spec.mesh on the edge: the warm target's allow entry, and no endpoints on an edge-only machine's node entries

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0296
- **Tags:** m5b, mesh, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0173](0173-warm-replication-path-the-source-node-ships-changed-ranges.md), [ADR 0266](0266-where-the-primary-takes-a-voter-s-mesh-endpoint-from-node.md)

## Context

Section 5.11 says allow is [] on the edge and also gives a warm target its allow entry; an edge+node machine can be a warm target. The endpoint row does not say an edge-only machine's entries for nodes carry none.

## Decision

spec.mesh on the edge: the warm target's allow entry, and no endpoints on an edge-only machine's node entries.

Gap. node-agent-protocol.md section 5.11, `spec.mesh`, says two things that do not fit an edge which also holds the warm copy, or an edge that does not vote.

1. **The `allow` row** says `allow` is `[]` on the edge. The same row also says that, from M5b, the target of a warm volume gets `{ source: <the source's mesh address>, address: <its own mesh address>, port: <the warm entry's port> }` (section 6.13).
   - A machine with both `edge` and `node` can be a warm node: ADR 0275 does not exclude the edge.
   - It also becomes the target after a failover, when it was the source.
   - On such a machine the two sentences conflict. Without the entry, its firewall drops the replication stream, and the warm copy stays `syncing` or goes `failed`.
2. **The `endpoint` row** (ADR 0266) reads as if every peer entry carried the peer's session address. Section 6.9 says the primary takes an endpoint "for the edge from M5a, and for every voter from M5b".
   - An edge-only machine is not a voter. Its entries for nodes keep the M5a shape, with no endpoint and no keepalive, and the nodes dial it.
   - The text does not say that outright.

Option A, which the maintainer chose (PR #671 already behaves this way):
- The `allow` row says "`[]` on the edge, except the entries of a warm volume's target", or "on any machine that is the target of a warm volume".
- The `endpoint` row adds: "an edge-only machine's entries for nodes carry none, as in M5a: the edge is no voter, and the nodes dial it".
- The cost is text only. There is no wire change, and the code already does this.

Option B: an edge machine is never a warm node (add "not the edge" to the eligibility of ADR 0275). Cost:
- A failover still makes an `edge`+`node` source the target, so the conflict comes back after one failover.
- A two-machine cluster of `edge`+`node` and `node` could not keep a warm copy on the edge.

Option C: an edge-only machine's entries for nodes carry the nodes' endpoints too. Cost:
- The node agent sets an endpoint when a peer is new and when the spec's endpoint changes. A node behind NAT would then get its NAT's address on the edge after every change.
- The edge already reaches every node through roaming, so this adds nothing.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0173](0173-warm-replication-path-the-source-node-ships-changed-ranges.md): on which machines a warm volume's target gets the allow rule for the replication port: on any machine, the edge included. The rest of [ADR 0173](0173-warm-replication-path-the-source-node-ships-changed-ranges.md) stands.
- Refines [ADR 0266](0266-where-the-primary-takes-a-voter-s-mesh-endpoint-from-node.md): which peer entries carry an endpoint: on a machine whose only role is edge, the entries for nodes carry no endpoint and no keepalive, as in M5a. The rest of [ADR 0266](0266-where-the-primary-takes-a-voter-s-mesh-endpoint-from-node.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0296 in the project decision ledger. Do not edit by hand; changes go through a new decision.
