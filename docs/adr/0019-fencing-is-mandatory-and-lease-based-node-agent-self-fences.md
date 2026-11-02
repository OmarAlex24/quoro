# ADR 0019: Fencing is mandatory and lease-based; node agent self-fences from M0

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0019
- **Tags:** reliability, node-agent, fencing
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Starting a stateful app on node B while it still runs isolated on A produces two diverging copies (data corruption).

## Decision

Fencing is mandatory and lease-based; node agent self-fences from M0.

A node that loses its lease stops its own stateful apps. The same lease mechanism serves primary election and per-app failover. The node-agent protocol must include self-fencing from M0 so it is not redesigned later.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0019 in the project decision ledger. Do not edit by hand; changes go through a new decision.
