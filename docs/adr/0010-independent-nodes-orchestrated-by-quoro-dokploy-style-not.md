# ADR 0010: Independent nodes orchestrated by Quoro (Dokploy-style), not Incus-style clustering

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0010
- **Tags:** architecture, node-agent
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Works across VPS from different providers over the internet; Incus-style distributed DB needs 3 nodes for quorum and suffers on WAN latency.

## Decision

Independent nodes orchestrated by Quoro (Dokploy-style), not Incus-style clustering.

Mesh between nodes over WireGuard (implementation to decide). Design principle: if the control plane dies, apps keep running.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0010 in the project decision ledger. Do not edit by hand; changes go through a new decision.
