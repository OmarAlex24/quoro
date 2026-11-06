# ADR 0238: A new failed fence of a lease whose acknowledgement is pending takes it out of fence_ack_pending and lists it again

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0238
- **Tags:** contracts, leases, fencing, m5b
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Section 8.5 step 4 says only a command_result ok clears fence_ack_pending. If the node's fence of an acknowledged lease fails again before the fence_ack reaches it (for example the session-start revoke at the new token), the pending fence_ack would clear the new failure that no operator has seen, and the node could start a stateful VM while the old one may still run.

## Decision

A new failed fence of a lease whose acknowledgement is pending takes it out of fence_ack_pending and lists it again.

The fence acknowledgement (#391) implements it: when a node.fence_failed event or a lease_revoke answered fence_failed names a lease that sits in nodes.fence_ack_pending, the primary removes that lease from fence_ack_pending and lists it in fence_failure, in the same transaction. A later fence_ack then carries only the leases still pending; the new failure needs a new human acknowledgement through POST /nodes/{node_id}/fence-ack. Section 8.5 step 4 of the node-agent protocol says so: a command_result with status ok clears only the delivered ids from fence_ack_pending, and a new failure of one of the pending leases (step 1) also clears it from there and lists it again. It supersedes nothing; it narrows step 4.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0238 in the project decision ledger. Do not edit by hand; changes go through a new decision.
