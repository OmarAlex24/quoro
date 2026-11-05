# ADR 0142: Replicas: App.replicas (1..eligible nodes, default 1), stateless apps only, one instance per node; refuse rather than under-place

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0142
- **Tags:** m5a, contracts, reliability
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Roadmap M5a DoD lines 1-2 and D-0020 (stateless apps get replicas behind the LB). Spike: docs/research/m5a-replicas-and-edge.md section 4.

## Decision

Replicas: App.replicas (1..eligible nodes, default 1), stateless apps only, one instance per node; refuse rather than under-place.

Contract A: openapi App/CreateApp/UpdateApp gain replicas (integer >=1, default 1); migration adds apps.replicas; validation_failed when fewer eligible nodes than replicas; conflict for replicas>1 on a stateful app; scheduler anti-affinity by node within placement.node_ids; a change of replicas is a state change, not a deployment; deployments roll one node at a time. Instances get QUORO_INSTANCE_ID and QUORO_NODE_ID env vars so the demo app can identify itself. Alternatives: per-node explicit instance list (more control, more API); replicas tied to ha_policy (conflates stateless scaling with data policy). Recommendation: the replicas count as above.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0142 in the project decision ledger. Do not edit by hand; changes go through a new decision.
