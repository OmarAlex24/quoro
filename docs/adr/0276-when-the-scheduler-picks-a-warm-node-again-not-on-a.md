# ADR 0276: When the scheduler picks a warm node again: not on a disconnect, only for a lasting reason

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0276
- **Tags:** m5b, warm, contracts, scheduler
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

WarmCopy.node_id lists 'connected' among the eligibility conditions and says node_id is null 'while no node is eligible', but nothing says when the scheduler picks again. Read literally, every short disconnect of the warm node makes another node the warm node: a full copy starts there, and the old node's copy is deleted as soon as the source is healthy (the deletion rule of #556), before the new full copy completes. A flapping node moves the copy back and forth, and a two-node cluster sets node_id to null on every disconnect.

## Decision

When the scheduler picks a warm node again: not on a disconnect, only for a lasting reason.

The warm node is chosen once and kept. The scheduler picks a warm node (1) when the policy becomes warm or an operator sets warm.node_id to null, among connected eligible nodes (connected stays a condition there and for ha_cost.warm.available); and (2) again only when the current warm node is no longer eligible for a lasting reason: removed, no longer the node role, no longer snapshots with reflink, has become the volume's node, or has no mesh address. A disconnected warm node stays the warm node; App.warm shows state failed with error 'not connected' (what #556 already derives), and the copy resumes incrementally when it returns. A pinned node keeps its pin until it is removed; then the pin is cleared and the scheduler picks. node_id is null only while the app is warm and no node is eligible after such a lasting loss. Contract text in WarmCopy.node_id, after 'Null while no node is eligible; state is then failed.': 'The scheduler keeps the warm node while it is disconnected and picks another only when it is removed, loses the node role, stops snapshotting with reflink or becomes the volume's node; an operator's pin lasts until the node is removed.' Rejected alternative: pick again after the warm node has been disconnected for a fixed time; costs a timer, a new constant in the contract, and a full copy after every long outage.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0276 in the project decision ledger. Do not edit by hand; changes go through a new decision.
