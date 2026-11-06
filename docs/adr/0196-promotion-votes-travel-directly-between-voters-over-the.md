# ADR 0196: Promotion votes travel directly between voters over the mesh, not through the edge

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0196
- **Tags:** m5b, contracts, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0169 says the candidate asks the voters over the mesh, but the M5a mesh is a star with the edge as hub; the milestone plan leaves the choice to the lease contract change.

## Decision

Promotion votes travel directly between voters over the mesh, not through the edge.

Every two voters are mesh peers unless both are witness-only (a peer the star already has is not repeated; allowed_ips is the other voter's mesh /32). Each voter's node agent listens on its own mesh address, TCP spec.votes.port (default 51821), HTTP/1.1 JSON, POST /v1/vote, and answers only requests from the mesh address of a voter with the node role in spec.votes.voters (others: not_allowed). The firewall rule follows from spec.votes, not from allow. Why not relay through the edge: it makes the edge a single point of failure for the operation used when a machine is gone, and a cluster with a separate witness may have no edge; WireGuard already authenticates the peer by key and address; warm replication needs node-to-node peers anyway (D-0144, D-0173). Limitation stated: two voters both behind NAT with no endpoint get no peer entry and cannot count each other.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0196 in the project decision ledger. Do not edit by hand; changes go through a new decision.
