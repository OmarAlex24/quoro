# ADR 0255: A conversion of an expiring lease to local is sent only once every possible successor sees the lease as local

- **Status:** Accepted; refined by [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md)
- **Date:** 2026-11-07
- **Ledger id:** D-0255
- **Tags:** m5b, leases, contracts, warm, election
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md)

## Context

Found in the safety review of the node side of ADR 0241 (#454). Section 8.4 makes the node adopt a same-token local grant for an expiring lease: it drops valid_until, stops renewing, and keeps the VMs running with no deadline. Local leases need no majority support (section 8.8), and lease_grant carries no term. So a deposed primary that has not yet seen stale_term (section 7.4) can end a lease's deadline. Scenario: partition {P, node X} | {Q, V2, V3}. Q is promoted after the horizon wait, and its state says the lease is expiring. On P's side, an updateApp moves app A off warm, and P sends X a same-token local grant. X was uncertain and about to fence; it now runs A indefinitely. Q opens its gate and fails A over to Y, so A runs on X and Y at once. The same holds from held. Before the node side of ADR 0241, the node refused such a grant, so X fenced.

## Decision

A conversion of an expiring lease to local is sent only once every possible successor sees the lease as local.

The primary sends the same-token local grant that ends an expiring lease's renewals only after the HA policy change is committed in the replicated change log on a majority of the voters, and while it still holds majority support (section 8.8). A successor primary therefore sees the lease as local and answers failover with lease_held, until the source is connected or declared lost. Until the lease row is local and replicated, the lease stays expiring and keeps renewing. The node's handling of the grant does not change (ADR 0241). Interim: no primary sends conversion grants until this and the ADR 0241 primary side land.

_Some wording was changed for the public repository._

## Consequences

- [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md) refines what replicated means: acknowledged by every machine with the node role instead of committed on a majority of the voters, and every local grant of this design needs majority support. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0255 in the project decision ledger. Do not edit by hand; changes go through a new decision.
