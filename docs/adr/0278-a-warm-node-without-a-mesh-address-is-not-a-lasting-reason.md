# ADR 0278: A warm node without a mesh address is not a lasting reason to pick another warm node; the contract sentence of D-0276 governs

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0278
- **Tags:** m5b, warm, contracts
- **Supersedes:** [ADR 0276](0276-when-the-scheduler-picks-a-warm-node-again-not-on-a.md)
- **Superseded by:** none
- **Related:** [ADR 0212](0212-the-scheduler-picks-the-warm-node-unless-an-operator-pins.md)

## Context

The maintainer, 2026-11-07, on a contradiction found in D-0276. Its body listed 'or has no mesh address' among the lasting reasons, while its approved contract sentence does not. Since D-0266, a voter has no endpoint until the primary has seen its session since the primary started, which is a transient condition, like a disconnect.

## Decision

A warm node without a mesh address is not a lasting reason to pick another warm node; the contract sentence of D-0276 governs.

Refines D-0276; the rest of D-0276 stands. The scheduler keeps the warm node while it is disconnected or has no mesh address. It picks another only when the warm node is removed, loses the node role, stops snapshotting with reflink, or becomes the volume's node. An operator's pin lasts until its node is removed. A warm node without a mesh address shows state failed, like a disconnected one, until its endpoint is known again.

_Some wording was changed for the public repository._

## Consequences

- Replaces ADR 0276.
- Refines [ADR 0212](0212-the-scheduler-picks-the-warm-node-unless-an-operator-pins.md): when the scheduler picks the warm node again: not on a disconnect or a missing mesh address, only for a lasting reason. The rest of [ADR 0212](0212-the-scheduler-picks-the-warm-node-unless-an-operator-pins.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0278 in the project decision ledger. Do not edit by hand; changes go through a new decision.
