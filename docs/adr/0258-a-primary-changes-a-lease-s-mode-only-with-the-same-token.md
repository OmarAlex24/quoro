# ADR 0258: A primary changes a lease's mode only with the same token

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0258
- **Tags:** m5b, leases, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md)

## Context

A third safety review of the node side of ADR 0241 and ADR 0242 (#454, #483) found this. Section 8.4's state machine accepts a higher-token grant whatever the mode of the lease the node holds: a higher-token expiring grant over a held local lease makes it uncertain and keeps valid_until, which a local lease does not have; a higher-token local grant over an expiring lease makes it held local and drops the deadline. Before the PR for #454 the node refused both, because a lease id named one mode. (1) Expiring over local: the primary's gate counts that grant (it is not a conversion), but the node has no deadline until a renewal is answered, so its VMs are stopped by the reconciler with no bound tied to the gate, and a start the old local lease permitted keeps its 60 s bound while the new token's fence would wait for it. (2) Local over expiring: the deadline of an expiring lease ends, also from a deposed primary that allocates a higher token.

## Decision

A primary changes a lease's mode only with the same token.

Contract text for sections 5.12 and 8.4: a mode change is always the same-token conversion of 'Changing the mode'. A primary never sends a higher-token grant whose mode differs from the lease the node holds (held or uncertain); after expired or revoked, a higher token may carry either mode. The node refuses such a grant with bad_message, as before #454; this optional part was taken. The token bump of a fence acknowledgement (ADR 0245) then uses the mode the node holds, not the row's, while a conversion is pending under ADR 0244.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md): which grants may change a lease's mode: only the same-token conversion. The rest of [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0258 in the project decision ledger. Do not edit by hand; changes go through a new decision.
