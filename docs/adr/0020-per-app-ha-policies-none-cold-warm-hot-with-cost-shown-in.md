# ADR 0020: Per-app HA policies: none / cold / warm / hot, with cost shown in the panel

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0020
- **Tags:** reliability, ha-policy
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Zero downtime requires reserving resources on other nodes; users choose the trade-off per app.

## Decision

Per-app HA policies: none / cold / warm / hot, with cost shown in the panel.

none: nothing on other nodes. cold: backups only (RPO since last backup, RTO = restore). warm: data continuously replicated, image pulled, stopped (RPO seconds, RTO = microVM boot; costs disk). hot: running + replicating (near-zero RPO/RTO; costs disk+RAM+CPU). Async replication by default; synchronous optional for nearby nodes (adds latency to every write). Suggested defaults: stateless apps get replicas behind the LB when >=2 nodes; stateful apps get warm. Stateless apps: replicas on several nodes, edge LB stops sending traffic to the dead node (zero downtime).

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0020 in the project decision ledger. Do not edit by hand; changes go through a new decision.
