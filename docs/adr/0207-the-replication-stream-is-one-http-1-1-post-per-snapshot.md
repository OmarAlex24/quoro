# ADR 0207: The replication stream is one HTTP/1.1 POST per snapshot over node-to-node mesh peers, framed binary, TCP 51822

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0207
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0173 fixes a token-carrying stream from the source to the target but not its transport or wire format; the milestone plan leaves it to the warm replication contract change.

## Decision

The replication stream is one HTTP/1.1 POST per snapshot over node-to-node mesh peers, framed binary, TCP 51822.

The two nodes of a warm volume are mesh peers (as ADR 0196 made every two voters peers) and the mesh allow rule admits the pair. The target listens on its mesh address, TCP spec.volumes[].warm.port (default 51822, next to the vote port 51821), POST /v1/replicate, and accepts only the source's peer_address. The body is a sequence of frames [u8 type][u32 BE length][payload]: 1 header (app_id, volume_id, lease_id, fencing_token, snapshot_seq, base_seq or null, taken_at, size_bytes), 2 block (offset u64, length u32 up to 1 MiB inside one cell, blake3 32 bytes, data), 3 zero range (offset u64, length u64), 4 manifest (block_count, root_hash). The answer is JSON {status complete|refused, snapshot_seq, reason}. Why not the backup chunk format: blocks apply in place at their offsets; FastCDC chunks do not (ADR 0173). Why HTTP over the mesh: the same stack as the vote listener, WireGuard authenticates the peer, no new TLS identity.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0207 in the project decision ledger. Do not edit by hand; changes go through a new decision.
