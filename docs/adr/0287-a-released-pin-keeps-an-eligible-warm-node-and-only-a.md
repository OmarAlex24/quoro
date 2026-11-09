# ADR 0287: A released pin keeps an eligible warm node, and only a reported pause_copy counts as no longer snapshotting with reflink

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0287
- **Tags:** m5b, warm, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0278](0278-a-warm-node-without-a-mesh-address-is-not-a-lasting-reason.md)

## Context

The maintainer, 2026-11-09, confirmed both readings made while implementing the warm policy. Both avoid shipping a whole volume again without need and follow the rule that only a lasting loss re-picks the warm node.

## Decision

A released pin keeps an eligible warm node, and only a reported pause_copy counts as no longer snapshotting with reflink.

1. When warm.node_id is set to null (the pin is released), the planner keeps the current warm node if it is still eligible, since it already holds the copy; otherwise it picks among the connected eligible nodes. 2. Only a node that reports an explicit pause_copy snapshot method counts as no longer using reflink; a snapshot method not yet reported (null) is transient and does not change the warm node.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0278](0278-a-warm-node-without-a-mesh-address-is-not-a-lasting-reason.md): when the warm node changes: a released pin keeps it while it is still eligible, and only a reported pause_copy counts as no longer snapshotting with reflink. The rest of [ADR 0278](0278-a-warm-node-without-a-mesh-address-is-not-a-lasting-reason.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0287 in the project decision ledger. Do not edit by hand; changes go through a new decision.
