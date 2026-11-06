# ADR 0212: The scheduler picks the warm node unless an operator pins it through UpdateAppRequest.warm

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0212
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0174 places a standby on the target but does not say how the target is chosen or changed.

## Decision

The scheduler picks the warm node unless an operator pins it through UpdateAppRequest.warm.

Eligible: a connected node with the node role, in placement.node_ids when set, not the volume's node, snapshotting with reflink, and reachable over the mesh from the source (two nodes both behind NAT cannot pair). UpdateAppRequest.warm {node_id (null = scheduler), interval_s 5-3600} is accepted only with ha_policy warm. A new warm node gets a full copy and the old copy is deleted. Stored as apps.warm_node_id, apps.warm_node_pinned and apps.warm_interval_s (migration 0011); the interval survives a policy change.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0212 in the project decision ledger. Do not edit by hand; changes go through a new decision.
