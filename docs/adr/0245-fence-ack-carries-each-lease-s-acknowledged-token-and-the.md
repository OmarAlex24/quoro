# ADR 0245: fence_ack carries each lease's acknowledged token, and the node clears only failed fences below it

- **Status:** Accepted; refined by [ADR 0246](0246-a-fence-started-by-a-lease-revoke-records-its-failure-with.md), [ADR 0247](0247-fence-ack-stores-current-token-1-for-a-listed-lease-another.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0245
- **Tags:** m5b, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0246](0246-a-fence-started-by-a-lease-revoke-records-its-failure-with.md), [ADR 0247](0247-fence-ack-stores-current-token-1-for-a-listed-lease-another.md)

## Context

A safety review of #430 found this. The node keys its failed fences by lease id only, and fence_ack names lease ids only. So an acknowledgement clears any failure of a listed lease that is recorded before the node takes the command, including one the operator never saw. Two cases reach this in M5b: (a) a fence of the lease ends, and fails, while the fence_ack waits in the service's queue; (b) a failed fence is recorded, but its node.fence_failed has not reached the primary yet (events flush every 1 s) when the ack arrives. Concrete case: the operator acknowledges L at token N, so the primary bumps it to N+1. L is granted again at N+2, the session drops, and L expires. Its fence F2 fails and is recorded. At the next session the hello reports L at N+2, the primary holds L at N+2 for this node, so no stale revoke goes out and the pending ack goes out right after welcome. The node flushes F2's event, then applies the ack and clears F2. The node is now not failed while F2's VM may still run; the primary lists L again (ADR 0238), but nothing ever marks the node failed again. This is older than #430 (the old refusal only covered fences still in flight) and not reachable today: no expiring lease is granted yet. ADR 0238 protects the primary's view, not the node's mark.

## Decision

fence_ack carries each lease's acknowledged token, and the node clears only failed fences below it.

In section 5.17 and section 8.5 steps 4 and 5, as a wire addition:
(1) fence_ack params gain fencing_tokens, aligned with lease_ids. Each entry is the token the acknowledgement bumped the lease to (section 8.5 step 3).
(2) The node records each failed fence with its token, as fence records already do. Each node.fence_failed carries it (the optional fencing_tokens of #436).
(3) On fence_ack the node clears, for each listed lease, only the failed fences whose token is below the acknowledged token. A failure at or above it stays, the node stays failed, and detail names it. The failure is already listed again on the primary through ADR 0238.
(4) A node that gets a fence_ack without fencing_tokens (an older primary) keeps today's behaviour.
Until the node implements it, it clears by lease id, and #430 states that limit in a comment.

_Some wording was changed for the public repository._

## Consequences

- [ADR 0246](0246-a-fence-started-by-a-lease-revoke-records-its-failure-with.md) refines which token a fence started by a lease_revoke records, and which listed leases a fence acknowledgement bumps. The rest of this decision stands.
- [ADR 0247](0247-fence-ack-stores-current-token-1-for-a-listed-lease-another.md) refines the token the acknowledgement stores for a listed lease another node holds. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0245 in the project decision ledger. Do not edit by hand; changes go through a new decision.
