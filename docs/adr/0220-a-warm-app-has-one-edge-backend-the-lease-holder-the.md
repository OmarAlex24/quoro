# ADR 0220: A warm app has one edge backend, the lease holder; the standby is never a backend

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0220
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The milestone plan says edge-served warm routes; how the edge treats the standby is open.

## Decision

A warm app has one edge backend, the lease holder; the standby is never a backend.

The edge balances over one backend for a warm app, the instance on the lease holder. On failover or switchover the backend moves with the lease; without an edge, the tunnel route moves with ADR 0168's grace window. The standby VM's image is prepared but the VM is never created until it becomes the source.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0220 in the project decision ledger. Do not edit by hand; changes go through a new decision.
