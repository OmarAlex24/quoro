# ADR 0213: failoverApp requires the warm node id and refuses with conflict reasons while two writers could exist

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0213
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0175 defines a manual failover with human approval for agents; the request shape and preconditions are open.

## Decision

failoverApp requires the warm node id and refuses with conflict reasons while two writers could exist.

POST /api/v1/apps/{app_id}/failover {node_id} where node_id must be App.warm.node_id. Refused with 409 conflict and reason not_warm, in_progress, no_copy, node_disconnected, fence_failed, no_support, or lease_held (the old holder's gate has not passed and no fence was confirmed; a local lease is held until declareNodeLost). A connected source is revoked first. Then one change grants the warm node an expiring lease with a higher token, starts the standby VM on the last copy, moves the volume and the domains, makes the old source the warm node, and emits app.failed_over. Problem.reason, so far only for forbidden, also carries these conflict reasons. Automatic failover stays M6.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0213 in the project decision ledger. Do not edit by hand; changes go through a new decision.
