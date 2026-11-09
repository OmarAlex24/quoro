# ADR 0254: A converted lease's first gate counts the conversion grant's duration

- **Status:** Accepted; refined by [ADR 0268](0268-a-grant-longer-than-a-lease-s-stored-duration-ms-waits.md)
- **Date:** 2026-11-07
- **Ledger id:** D-0254
- **Tags:** m5b, leases, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md), [ADR 0268](0268-a-grant-longer-than-a-lease-s-stored-duration-ms-waits.md)

## Context

A safety review of the node side of ADR 0241 (#454) found this. Section 8.4 gives a converted lease a provisional deadline from its first renewal, t_send + D - M with D from the conversion grant, and says any answer sets a valid_until no earlier. But the primary's gate excludes the conversion grant, and an answer may carry any duration at or above the floor. So the first gate is T_A1 + D_A1 + 2M(D_A1), with nothing tying D_A1 to D_conv. If the first answer carries a shorter duration and is lost in a partition, the gate opens before the node fences. Example: conversion sent during a maintenance window with W = 180 s, first answer with D = 15 s, answer lost. The node fences by t_send + 180 s, while the gate opens at about T_A1 + 18 s, so the app runs on two nodes for about 160 s. Without a window, D_conv = 15 000 and D_A1 = 10 000 already overshoots by about 3 s. The node cannot know D_A1 until the answer arrives, so the rule has to be on the primary's side.

## Decision

A converted lease's first gate counts the conversion grant's duration.

The primary's side is implemented with the primary side of ADR 0241; the node needs no change. (1) For a converted lease, the gate the primary opens from the first answer is T_grant + max(D_conv, D_A1) + 2M(max(D_conv, D_A1)), where D_conv is the conversion grant's duration_ms; later answers count as today. (2) Before it sends a conversion grant, the primary raises leases.duration_ms to at least D_conv, so a restarted primary's first gate (start + duration_ms + 2M) also covers it. Alternative considered: the node computes the provisional deadline with the 3 000 ms floor; rejected because a converted warm app would then fence 3 s after its first renewal whenever the answer is late by 2 s.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md): the gate that the first answer after a conversion opens. The rest of [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md) stands.
- [ADR 0268](0268-a-grant-longer-than-a-lease-s-stored-duration-ms-waits.md) refines when a conversion grant longer than the stored duration_ms is sent: only once the raise before it is replicated. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0254 in the project decision ledger. Do not edit by hand; changes go through a new decision.
