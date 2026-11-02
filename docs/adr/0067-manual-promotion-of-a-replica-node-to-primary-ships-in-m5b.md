# ADR 0067: Manual promotion of a replica node to primary ships in M5b, alongside leases and fencing

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0067
- **Tags:** roadmap, reliability
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendations that closed these open questions on 2026-11-02. Promotion must first fence the old primary's leases (D-0019); M5b is where witness, leases and fencing land.

## Decision

Manual promotion of a replica node to primary ships in M5b, alongside leases and fencing.

Roadmap: add a manual-promotion DoD item to M5b; promotion refuses while the old primary may still hold expiring leases unless an operator acknowledges the fence.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0067 in the project decision ledger. Do not edit by hand; changes go through a new decision.
