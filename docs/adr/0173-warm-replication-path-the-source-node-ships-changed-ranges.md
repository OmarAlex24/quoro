# ADR 0173: Warm replication path: the source node ships changed ranges straight to the target node over the WireGuard mesh, as BLAKE3-hashed blocks under a per-snapshot manifest; the target applies them to a staging clone and swaps it in only when complete; every message carries the lease token

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0173
- **Tags:** m5b, sync, node-agent, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0023 sends warm replication node to node through the same chunk engine; D-0144 deferred node-to-node mesh peers to M5b. The target must always hold one complete, consistent copy (the 'last shipped snapshot' of the DoD), and a stale source must not be able to overwrite it (D-0047 tokens, node-agent-protocol 8.6).

## Decision

Warm replication path: the source node ships changed ranges straight to the target node over the WireGuard mesh, as BLAKE3-hashed blocks under a per-snapshot manifest; the target applies them to a staging clone and swaps it in only when complete; every message carries the lease token.

Mesh: spec.mesh gains node-to-node peers for each warm pair and an allow rule for the replication port, from the source's mesh address to the target's host. Wire: a replication stream on the mesh (new protocol section): snapshot header {app_id, volume_id, lease_id, fencing_token, snapshot_seq, size}, then blocks {offset, length <= 1 MiB, blake3, bytes}, then a manifest {block count, root hash over the full block-hash map}. The target rejects any stream whose token is lower than the highest it has seen for the app and records the rejection; it writes into a reflink clone of its last complete copy, verifies the root hash, then renames it into place and reports snapshot_seq, time and lag in status_report. Blocks are not FastCDC chunks: they apply in place, and WireGuard already authenticates and encrypts the link; the hashing and IO code is the chunk engine crate's. Interval default 30 s, minimum 5 s; the lab spike of ADR 0172 sets the final default. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0173 in the project decision ledger. Do not edit by hand; changes go through a new decision.
