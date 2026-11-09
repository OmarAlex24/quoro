# ADR 0295: A restarted primary sends a marked row's pending new-token grant at the named holder's next session start, whatever the row's mode

- **Status:** Accepted; refined by [ADR 0299](0299-a-node-s-hello-counts-toward-a-marked-row-s-grant-only-once.md)
- **Date:** 2026-11-09
- **Ledger id:** D-0295
- **Tags:** m5b, leases, contracts, warm, failover
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md), [ADR 0292](0292-failoverapp-answers-202-at-once-while-the-grant-of-a-marked.md), [ADR 0299](0299-a-node-s-hello-counts-toward-a-marked-row-s-grant-only-once.md)

## Context

Found by the independent review of the contract change for D-0292. A failover's expiring grant to the warm node that waits under 'Grants of a marked row' is held only in the primary's memory. If the primary restarts before sending it, the row names the warm node at the new token, the warm node's hello does not list the lease, and step 4 of 'The holder fenced first' resends only a grant that names the node local. So nothing is ever sent: the app stays stopped until an operator acts, Lease.grant_pending reads false while unsettled stays true, and a second failover is no way out, because the failover already made the old source the warm node. A restore's new-token grant held back across a restart has the same gap. The stall predates D-0292; the contract change for D-0292 states it as a limit.

## Decision

A restarted primary sends a marked row's pending new-token grant at the named holder's next session start, whatever the row's mode.

Options. (a), which the maintainer chose: extend step 4 of 'The holder fenced first' (node-agent-protocol.md section 8.4) from rows naming the node local to any marked row whose newest write names that node as holder at a token above the one the node reports, in either mode. At that node's session start the primary sends that grant instead of the revoke of section 5.1, under 'Grants of a marked row' (replicated over the required set, majority support), with the same conditions as today: revoked_at null, the lease in neither fence_failure nor fence_ack_pending, and nothing sent at a session start without majority support. Until it is sent, Lease.grant_pending is true again and grant_waiting_on lists the nodes it waits for, so D-0292's polling states hold across a restart. Safety: this sends the grant the row already decided, after the same replication and support, to the node the row names; the old holder's lease was revoked, fenced or declared lost before the failover wrote the row. Cost: one more case in step 4; no new field and no migration. (b) Keep the stated limit, and name the operator's way out: today none is clear, since a failover to the old source is refused with node_disconnected and a restore is refused while the marked row's holder is unknown. (c) Persist held grants in a new column; heavier than (a), and (a) needs nothing stored because the row already says which grant is pending.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md): which marked rows step 4 of 'The holder fenced first' sends a grant for: any marked row whose newest write names the node as holder at a higher token than the node reports, in either mode, or a lease the node does not list. The rest of [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md) stands.
- Refines [ADR 0292](0292-failoverapp-answers-202-at-once-while-the-grant-of-a-marked.md): what a primary restart does to a failover's waiting grant: the grant is sent at the warm node's next session start, and grant_pending is true again until then. The rest of [ADR 0292](0292-failoverapp-answers-202-at-once-while-the-grant-of-a-marked.md) stands.
- [ADR 0299](0299-a-node-s-hello-counts-toward-a-marked-row-s-grant-only-once.md) refines what step 4 does when it sends: it checks its conditions again, and a node that lists the lease in the other mode at a lower token is revoked at that token first. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0295 in the project decision ledger. Do not edit by hand; changes go through a new decision.
