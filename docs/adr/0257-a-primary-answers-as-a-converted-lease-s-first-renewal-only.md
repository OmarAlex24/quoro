# ADR 0257: A primary answers, as a converted lease's first renewal, only a lease_renew the node sent after it took the conversion

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0257
- **Tags:** m5b, leases, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0244](0244-a-converted-lease-s-row-keeps-mode-local-until-the-primary.md)

## Context

A second safety review of the node side of ADR 0241 (#454) found this. The node computes a converted lease's provisional deadline from the t_send of the first renewal it sends after it takes the conversion. The primary opens the first gate from whichever renewal it answers first. A renewal from an earlier expiring period, sent before a same-token local grant reached the node, can reach the primary after it sent a later conversion when the HA policy flips twice within a session's delay. The node matches the answer to nothing (ADR 0256), but the primary counts it: the gate is T_answer(R_old) + D + 2M, while the node fences at t_send(R_new) + D - M, and R_new is not causally before T_answer(R_old). If the session breaks before R_new is answered, the gate opens before the node fences whenever the conversion took longer than 3M to reach the node. Leaving unanswered a renewal of a lease whose row the primary made local does not settle it either, because ADR 0244 keeps the row local until the first answer.

## Decision

A primary answers, as a converted lease's first renewal, only a lease_renew the node sent after it took the conversion.

Contract text for section 8.4 'Changing the mode', primary side: after it sends a same-token local grant for a lease, the primary sends that node a ping, and it sends no conversion grant for the lease until the node's answer to that ping arrives. The session is ordered both ways (section 4.2), so by then every renewal the node sent before it took the local grant has arrived. A renewal that arrives after the conversion grant was sent is then one the node sent after it took the conversion. The node needs no change: an answer that matches no outstanding renewal changes nothing, and since the PR for #454 an answer that arrives past its own deadline ends a provisional deadline. Alternative considered: a conversion counter carried in lease_renew; rejected because it changes the wire format for a case the ping barrier covers.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0244](0244-a-converted-lease-s-row-keeps-mode-local-until-the-primary.md): which renewal the primary answers as a converted lease's first one. The rest of [ADR 0244](0244-a-converted-lease-s-row-keeps-mode-local-until-the-primary.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0257 in the project decision ledger. Do not edit by hand; changes go through a new decision.
