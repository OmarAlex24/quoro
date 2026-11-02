# ADR 0084: M1 keeps every microVM isolated from every other; an additive contract change carrying a project or isolation group on NetAttach and guest DNS on NodeNetwork lands in a standalone PR at the start of M2, before any multi-VM app feature

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0084
- **Tags:** engine, networking, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-02. The frozen Engine trait has no carrier for a VM's project or its DNS servers; full isolation is the safe side of engine-trait.md 3.3.

## Decision

M1 keeps every microVM isolated from every other; an additive contract change carrying a project or isolation group on NetAttach and guest DNS on NodeNetwork lands in a standalone PR at the start of M2, before any multi-VM app feature.

Until then the CH engine isolates all VMs (bridge port isolation plus an nftables forward drop) and takes guest DNS from its own node configuration. Same-project VM-to-VM traffic is impossible in M1; this is documented, not a bug.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0084 in the project decision ledger. Do not edit by hand; changes go through a new decision.
