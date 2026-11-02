# ADR 0012: Same binary on every node; one primary is the single source of truth, others are replicas

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0012
- **Tags:** architecture, control-plane, reliability
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer's idea: every node has its own API, one is master, everything is copied to it; keeps Quoro truly self-hosted (porting the control plane to Cloudflare Workers/Vercel would defeat self-hosting).

## Decision

Same binary on every node; one primary is the single source of truth, others are replicas.

Binary includes API + panel + engine. Roles: node, witness, edge (combinable). All writes go through the primary; replicas hold a copy and serve reads, and forward writes to the primary. If the primary dies, apps keep running; only changes are blocked until a replica is promoted. The static panel can be hosted anywhere, but the stateful API stays self-hosted.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0012 in the project decision ledger. Do not edit by hand; changes go through a new decision.
