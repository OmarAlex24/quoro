# ADR 0212: The scheduler picks the warm node unless an operator pins it through UpdateAppRequest.warm

- **Status:** Accepted; refined by [ADR 0275](0275-a-warm-app-s-placement-does-not-narrow-its-warm-node-a.md), [ADR 0278](0278-a-warm-node-without-a-mesh-address-is-not-a-lasting-reason.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0212
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0275](0275-a-warm-app-s-placement-does-not-narrow-its-warm-node-a.md), [ADR 0278](0278-a-warm-node-without-a-mesh-address-is-not-a-lasting-reason.md)

## Context

ADR 0174 places a standby on the target but does not say how the target is chosen or changed.

## Decision

The scheduler picks the warm node unless an operator pins it through UpdateAppRequest.warm.

Eligible: a connected node with the node role, in placement.node_ids when set, not the volume's node, snapshotting with reflink, and reachable over the mesh from the source (two nodes both behind NAT cannot pair). UpdateAppRequest.warm {node_id (null = scheduler), interval_s 5-3600} is accepted only with ha_policy warm. A new warm node gets a full copy and the old copy is deleted. Stored as apps.warm_node_id, apps.warm_node_pinned and apps.warm_interval_s (migration 0011); the interval survives a policy change.

## Consequences

- [ADR 0275](0275-a-warm-app-s-placement-does-not-narrow-its-warm-node-a.md) refines which nodes are eligible as warm node: placement.node_ids no longer narrows them. The rest of this decision stands.
- [ADR 0278](0278-a-warm-node-without-a-mesh-address-is-not-a-lasting-reason.md) refines when the scheduler picks the warm node again: not on a disconnect or a missing mesh address, only for a lasting reason. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0212 in the project decision ledger. Do not edit by hand; changes go through a new decision.
