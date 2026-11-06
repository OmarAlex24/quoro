# ADR 0246: A fence started by a lease_revoke records its failure with the revoke's token; a fence acknowledgement bumps only the listed leases the node still holds

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0246
- **Tags:** m5b, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0245](0245-fence-ack-carries-each-lease-s-acknowledged-token-and-the.md)

## Context

Both readings of the contract change for ADR 0245 were approved on 2026-11-06. Recording the revoke's token (at or above the node's) keeps a failure the operator never saw from falling below the acknowledged token. Bumping only the leases the node still holds matches section 8.3 (declaring a node lost) and the code from fence_ack (#423); bumping a lease now held by another node would invalidate its new holder.

## Decision

A fence started by a lease_revoke records its failure with the revoke's token; a fence acknowledgement bumps only the listed leases the node still holds.

Section 8.5 step 2: the token recorded for a fence started by a lease_revoke is the revoke's fencing_token. Section 8.5 step 3: the primary increments the fencing_token of each listed lease that the node still holds; a lease already held by another node keeps its token. Both go into the contract change for ADR 0245 (#443).

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0245](0245-fence-ack-carries-each-lease-s-acknowledged-token-and-the.md): which token a fence started by a lease_revoke records, and which listed leases a fence acknowledgement bumps. The rest of [ADR 0245](0245-fence-ack-carries-each-lease-s-acknowledged-token-and-the.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0246 in the project decision ledger. Do not edit by hand; changes go through a new decision.
