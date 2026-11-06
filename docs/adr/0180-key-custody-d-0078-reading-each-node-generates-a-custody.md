# ADR 0180: Key custody (D-0078 reading): each node generates a custody key pair (X25519) and sends its public half in hello; the primary seals the cluster signing key, the DEK and the primary's TLS key to each node with HPKE in a replicated table; only role node machines get them

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0180
- **Tags:** m5b, security, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0078 replicates both cluster keys 'wrapped to that node's node-local key', but the node-local key of D-0050 is a symmetric key the primary never sees, so there is nothing to wrap to. The contracts, architecture and threat model still call custody open (README 98 and 330-332, architecture 302/747/775, db-schema 54, protocol 7, threat model 8.2/8.3).

## Decision

Key custody (D-0078 reading): each node generates a custody key pair (X25519) and sends its public half in hello; the primary seals the cluster signing key, the DEK and the primary's TLS key to each node with HPKE in a replicated table; only role node machines get them.

A dedicated X25519 key, not the WireGuard key (no cross-protocol key reuse) and not the Ed25519 node key. hello gains custody_public_key; a replicated table node_key_custody {node_id, key_kind (signing|dek|tls), key_id, sealed} holds HPKE base-mode seals (X25519-HKDF-SHA256, ChaCha20-Poly1305; the hpke crate is MIT/Apache, within D-0080). Every follower holds every seal but can open only its own. Edge-only and witness-only machines never get one. Re-sealed when a key rotates or a node enrols; removed with the node. A text-alignment contract PR updates the documents that still say 'open'. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0180 in the project decision ledger. Do not edit by hand; changes go through a new decision.
