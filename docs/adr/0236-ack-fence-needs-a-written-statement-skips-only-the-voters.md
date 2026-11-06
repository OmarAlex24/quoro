# ADR 0236: --ack-fence needs a written statement, skips only the voters' wait, declares the old primary's node lost, and keeps every lease's own gate

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0236
- **Tags:** m5b, contracts, promotion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0182 lets the operator acknowledge the fence instead of waiting, and ADR 0198 says that declares the old primary's node lost, but a reader could take the flag as permission to grant every lease at once.

## Decision

--ack-fence needs a written statement, skips only the voters' wait, declares the old primary's node lost, and keeps every lease's own gate.

quoro cluster promote --ack-fence requires --statement, kept as the cluster.promoted record's justification. It skips only the wait for the voters' lease horizon and, in the write that moves the primary status, declares the old primary's node lost as declareNodeLost does. The new primary still waits for each expiring lease's gate, counted from its own first start with the lease's stored duration_ms, before granting that lease elsewhere. The residual, stated in the threat model: an expiring grant the old primary made and the candidate never followed is unknown to the new primary, which may issue the same token again. Rejected: a per-term floor on new tokens, a mechanism this milestone does not need while the voters' wait covers the case.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0236 in the project decision ledger. Do not edit by hand; changes go through a new decision.
