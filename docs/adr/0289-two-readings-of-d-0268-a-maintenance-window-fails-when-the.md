# ADR 0289: Two readings of D-0268: a maintenance window fails when the raise is not replicated within D/3 plus one margin, and a conversion with no stored duration_ms waits for the raise

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0289
- **Tags:** m5b, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0268](0268-a-grant-longer-than-a-lease-s-stored-duration-ms-waits.md)

## Context

The maintainer, 2026-11-09, ratified the two readings written in D-0268's contract change. Both reuse bounds that already exist, and neither sends a grant without the support it needs.

## Decision

Two readings of D-0268: a maintenance window fails when the raise is not replicated within D/3 plus one margin, and a conversion with no stored duration_ms waits for the raise.

Refines D-0268. 1. A maintenance window fails when the raise of the lease duration is not replicated within D/3 plus one margin, the bound the command already applies to a holder's renewal. 2. A conversion of a lease whose row has no stored duration_ms waits for the raise before its grant goes out, because there is no shorter duration to fall back to.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0268](0268-a-grant-longer-than-a-lease-s-stored-duration-ms-waits.md): what happens while a node does not hold the raise: a maintenance window fails when the raise is not replicated within D/3 plus one margin, and a conversion with no stored duration_ms waits for the raise. The rest of [ADR 0268](0268-a-grant-longer-than-a-lease-s-stored-duration-ms-waits.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0289 in the project decision ledger. Do not edit by hand; changes go through a new decision.
