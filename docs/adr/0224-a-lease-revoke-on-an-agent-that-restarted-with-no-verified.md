# ADR 0224: A lease_revoke on an agent that restarted with no verified desired-state snapshot stops every app VM on the node; the contract gets a section 6.3 exception

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0224
- **Tags:** m5b, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

With no snapshot the node cannot tell which VMs are bound to the revoked lease; stopping every app VM is the safe side against a double run. The case is rare (no valid persisted snapshot plus a revoke with no recorded fence) and the reconciler starts the VMs of other apps again once the desired state arrives.

## Decision

A lease_revoke on an agent that restarted with no verified desired-state snapshot stops every app VM on the node; the contract gets a section 6.3 exception.

Section 6.3 says an agent restart never restarts VMs; it gains an exception for this case, in the same standalone contract change as ADR 0222. The implementation adds a test that the VMs of other leases and stateless apps are started again after the first desired_state verifies. Builder VMs are not stopped.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0224 in the project decision ledger. Do not edit by hand; changes go through a new decision.
