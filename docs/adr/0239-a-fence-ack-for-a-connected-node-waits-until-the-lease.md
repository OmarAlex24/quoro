# ADR 0239: A fence_ack for a connected node waits until the lease_revokes sent on that session are answered

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0239
- **Tags:** contracts, leases, fencing, m5b
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Section 8.5 step 4 says the fence_ack goes out 'at once when the node is connected'. The node answers a lease_revoke before it processes a command (4.2 rule 3, ADR 0222), so a fence_ack sent while a revoke of the same lease is unanswered is applied after that revoke's fence. If that fence fails, the node records a new failed fence and the fence_ack then clears it unseen. Waiting for the revoke's answer lets a fence_failed answer take the lease out of the acknowledgement first (ADR 0238).

## Decision

A fence_ack for a connected node waits until the lease_revokes sent on that session are answered.

The fence acknowledgement (#391) implements it: the primary tracks the message id of every lease_revoke it sends on a session; a fence_ack (live or at session start) goes out only once none is unanswered. In practice the wait is one fence's stop budget. Section 8.5 step 4 of the node-agent protocol says so: the fence_ack goes out at once when the node is connected and has answered every lease_revoke of the session, and otherwise at its next session as before.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0239 in the project decision ledger. Do not edit by hand; changes go through a new decision.
