# ADR 0222: Rule 3 of section 4.2 lets renewal answers for other leases through while a revoke's fence runs

- **Status:** Accepted; refined by [ADR 0237](0237-let-support-request-through-while-a-revoke-s-fence-runs.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0222
- **Tags:** m5b, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0237](0237-let-support-request-through-while-a-revoke-s-fence-runs.md)

## Context

From M5b one session carries both lease_revoke and the lease_grant answers to lease_renew. Rule 3 makes the node answer a revoke before it processes any later message. A local lease's fence has no deadline and takes up to T_grace + T_force (10 s + 5 s in the agent today). An expiring lease (D = 15 s, renewing every 5 s) whose renewal answer waits behind that fence can reach valid_until and self-fence even though the primary renewed it: a needless outage of a warm app, repeated on every slow local fence.

## Decision

Rule 3 of section 4.2 lets renewal answers for other leases through while a revoke's fence runs.

While a revoke's self-fence runs, the node keeps processing lease_grant messages for leases other than the revoked one (including renewal answers) and liveness traffic (heartbeat/ping), and holds back every other message - desired_state, lease_revoke for the same lease, commands, stream_open - until the revoke is answered. Rule 3 exists so that nothing after the revoke can start the revoked lease's VMs before they are stopped; a grant for another lease cannot, and the node already keeps a per-lease start gate and refuses to start anything for a lease whose fence is running or unrecorded (PR #385). The alternative, not chosen, kept rule 3 literal and bounded the local fence that answers a revoke inline to well under D/3 (e.g. T_grace 3 s, T_force 1 s for local leases), at the cost of shorter graceful shutdowns for stateful local apps. The maintainer accepted the first option on 2026-11-06. It is needed before the renewal loop (#75) ships.

_Some wording was changed for the public repository._

## Consequences

- [ADR 0237](0237-let-support-request-through-while-a-revoke-s-fence-runs.md) refines the messages a node processes while a revoke's fence runs (on a voter, support_request as well). The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0222 in the project decision ledger. Do not edit by hand; changes go through a new decision.
