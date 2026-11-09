# ADR 0256: An expiring grant that carries re never converts a lease (part 1 of 2)

- **Status:** Accepted; refined by [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md)
- **Date:** 2026-11-07
- **Ledger id:** D-0256
- **Tags:** m5b, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md), [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md)

## Context

A safety review of the node side of ADR 0241 found this. After a same-token local grant ends an expiring lease's renewals, a late expiring answer (re = a renewal sent before the local grant) carries the other mode, so section 5.12 makes the node convert the lease back to expiring. The lease then awaits a first renewal with no deadline, while the primary's view is local. If the session drops before that renewal leaves, the node holds the lease with no deadline. If the primary's answer was built from its state before the local grant and counted toward a gate, the node never fences while that gate is open. The contract does not say how the primary answers a renewal sent before a mode change.

## Decision

An expiring grant that carries re never converts a lease (part 1 of 2).

Contract text, sections 5.12 and 8.4 'Changing the mode': (1) A conversion grant answers no renewal and carries no re. An expiring lease_grant that carries re is an answer: when it answers no outstanding renewal, it changes nothing, whatever the lease's mode. The node side implements this with the ADR 0241 PR. (2) How the primary answers a renewal of a lease whose row it has made local is decided here too, and is written into the contract with the rule for ending an expiring lease's renewals; until then this ADR covers part (1) only.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md): which same-token grant converts a lease: one without re. The rest of [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md) stands.
- [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md) refines the reading of part 2: the primary answers no renewal of the lease once it has sent the local grant on that session. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0256 in the project decision ledger. Do not edit by hand; changes go through a new decision.
