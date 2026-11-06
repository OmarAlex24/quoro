# ADR 0175: Manual failover of a warm app: POST /apps/{app_id}/failover {node_id}, destructive (JIT approval for agents), refused with conflict while the source's lease may still be valid; grants an expiring lease with a higher token to the target

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0175
- **Tags:** m5b, control-plane, contracts, reliability
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The DoD has an operator, or an agent with human approval, start the warm copy only after the lease is lost; automatic per-app failover is M6 (D-0033).

## Decision

Manual failover of a warm app: POST /apps/{app_id}/failover {node_id}, destructive (JIT approval for agents), refused with conflict while the source's lease may still be valid; grants an expiring lease with a higher token to the target.

New scope app:failover, destructive (permission-model 4.1): humans with owner or operator act directly; an agent needs a JIT approval, which is the DoD's 'agent with human approval'. The primary refuses with conflict and reason lease_held until the gate of the source's last grant has passed (T_grant + D + 2M, node-agent-protocol 8.4), unless the source's fence was confirmed (node.fenced or a revoke answered). Then it bumps the token, grants an expiring lease to the target, moves the standby VM to running and switches the route; the old source's lease is revoked at its next hello (section 9). Events app.failed_over; audit operation failoverApp. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0175 in the project decision ledger. Do not edit by hand; changes go through a new decision.
