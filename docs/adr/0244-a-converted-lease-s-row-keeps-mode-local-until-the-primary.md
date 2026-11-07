# ADR 0244: A converted lease's row keeps mode local until the primary answers its first renewal, and is written expiring then

- **Status:** Accepted; refined by [ADR 0257](0257-a-primary-answers-as-a-converted-lease-s-first-renewal-only.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0244
- **Tags:** m5b, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md), [ADR 0257](0257-a-primary-answers-as-a-converted-lease-s-first-renewal-only.md)

## Context

A restarted primary treats every expiring lease as granted at start, so it could open a gate for a converted lease whose first renewal it never answered; a partitioned holder that never got the conversion would then run the app twice after a failover. Keeping the row local closes this with no migration and one write per conversion.

## Decision

A converted lease's row keeps mode local until the primary answers its first renewal, and is written expiring then.

Refines ADR 0241. The lease row's mode stays local after a change of ha_policy until the primary answers the holder's first renewal; that answer writes mode expiring in the same transaction. A restarted primary sees such a lease as local, so failover answers lease_held, and the holder's hello (mode local) makes the primary send the conversion grant again. The contract text goes with the contract change for ADR 0240 to ADR 0242 (#435); the implementation goes with the work on ADR 0241.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md): what the lease row records during a conversion: mode stays local until the primary answers the first renewal. The rest of [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md) stands.
- [ADR 0257](0257-a-primary-answers-as-a-converted-lease-s-first-renewal-only.md) refines which renewal the primary answers as a converted lease's first one. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0244 in the project decision ledger. Do not edit by hand; changes go through a new decision.
