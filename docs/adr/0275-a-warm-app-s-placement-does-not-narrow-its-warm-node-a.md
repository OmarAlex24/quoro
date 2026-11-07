# ADR 0275: A warm app's placement does not narrow its warm node: a stateful app's placement names only its volume's node

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0275
- **Tags:** m5b, warm, contracts, scheduler
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0174](0174-warm-copies-the-scheduler-picks-the-target-among-the-app-s.md), [ADR 0212](0212-the-scheduler-picks-the-warm-node-unless-an-operator-pins.md)

## Context

This gap blocks the warm policy. WarmCopy.node_id (openapi.yaml) and D-0212 make a node eligible as warm node only when it is 'in placement.node_ids when that is not empty, other than the node of the app's volume', and ADR 0174 says the scheduler picks 'among the app's placement nodes'. But creating an app's volume pins its placement to [the volume's node] (ADR 0145, service/volumes.rs), and updateApp refuses any placement change of a stateful app (conflict). Warm needs a volume, so every candidate app is stateful, its placement is exactly [volume node], and the eligible set is always empty: ha_cost.warm would always read no_warm_node and updateApp would refuse warm for every app.

## Decision

A warm app's placement does not narrow its warm node: a stateful app's placement names only its volume's node.

Options. A: placement does not narrow the warm node. Eligible: a connected node with the node role, other than the volume's node, that snapshots with reflink and has a mesh address (reachable, D-0266). An operator who wants a particular node pins it with UpdateAppRequest.warm.node_id. Contract text: in WarmCopy.node_id replace 'in placement.node_ids when that is not empty, ' with nothing, and add 'A stateful app's placement names only its volume's node (ADR 0145), so it does not narrow the choice; pin a node to choose it.' Same in D-0212's reading. This ADR supersedes ADR 0174's phrase 'among the app's placement nodes' (keep the rest of ADR 0174). The pairing code on main (warm::load_warm_apps, #556) already ignores placement, so only the choice and the eligibility check follow. B: a warm app's placement may name more nodes than the volume's node (updateApp accepts placement for a warm app, the warm node must be in it). Costs: unpins ADR 0145; deploy::pick_node places instances by placement, so a redeploy could put the VM on a node without the volume unless instance placement is changed too; failover must then also rewrite placement. Option A was chosen; its sentence goes with the contract change for ADR 0265, in the same openapi.yaml area.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0174](0174-warm-copies-the-scheduler-picks-the-target-among-the-app-s.md): which nodes the scheduler picks a warm copy's target among: the app's placement no longer narrows it. The rest of [ADR 0174](0174-warm-copies-the-scheduler-picks-the-target-among-the-app-s.md) stands.
- Refines [ADR 0212](0212-the-scheduler-picks-the-warm-node-unless-an-operator-pins.md): which nodes are eligible as warm node: placement.node_ids no longer narrows them. The rest of [ADR 0212](0212-the-scheduler-picks-the-warm-node-unless-an-operator-pins.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0275 in the project decision ledger. Do not edit by hand; changes go through a new decision.
