# ADR 0078: A promoted replica obtains the cluster signing and data-encryption keys because both are replicated to every replica node, wrapped to that node's node-local key

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0078
- **Tags:** security, reliability
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-02. Keeps manual promotion (M5b) simple and preserves the signing-key pins nodes took at enrollment (D-0050); re-keying on every promotion would force operator re-pins during an incident.

## Decision

A promoted replica obtains the cluster signing and data-encryption keys because both are replicated to every replica node, wrapped to that node's node-local key.

Option (a) of the contracts README/architecture section 18. Wrapped keys travel in replicated state; a node's node-local key never leaves it. Re-keying stays available as a deliberate operator action (signing_key_rotate). Implement before M5b.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0078 in the project decision ledger. Do not edit by hand; changes go through a new decision.
