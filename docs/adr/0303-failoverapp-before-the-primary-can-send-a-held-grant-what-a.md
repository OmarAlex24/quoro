# ADR 0303: failoverApp before the primary can send a held grant: what a failover answers when its grant must wait for replication

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0303
- **Tags:** m5b, failover, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md), [ADR 0292](0292-failoverapp-answers-202-at-once-while-the-grant-of-a-marked.md)

## Context

The primary side of failoverApp cannot follow the contract on main for every row. ADR 0292 says a failover whose grant waits writes the row (warm node, new token, mark set), commits the move, answers 202, and sends the grant once the write is replicated and support holds. On main, grant_app_lease returns NewGrant::Held and writes nothing for a marked row, a raise (a row that does not read expiring for at least D) or missing support, and nothing on main counts replication (#697 is open and keeps live positions only). So no code can write a held grant, send it later, or derive grant_pending/grant_waiting_on. Restore and regrant have the same hole: a marked row's restore never completes either.

## Decision

failoverApp before the primary can send a held grant: what a failover answers when its grant must wait for replication.

Q1. Marked rows and raises until the held-grant machinery exists. Option A: build the shared 'Grants of a marked row' machinery first (write the row, keep the grant held, send it once the write is replicated over the node-role machines and support holds, step 4 at session start, grant_pending/grant_waiting_on), on top of #697. It serves restore and regrant too, so it belongs with the lease work. failoverApp lands on it. Option B, which the maintainer chose for now: land failoverApp for rows grant_app_lease can grant at once (unmarked, expiring with duration_ms >= D, support held). A failover whose grant would be Held answers 409 no_support before any revoke or write, with a detail naming the wait, until A lands. No real build reaches that answer before #686: every build answers no_support at the support check first. B deviates from ADR 0292 for marked rows, so it needs the maintainer. Q2. A contract gap that remains after A: a failover over a row that does not read expiring for at least D (every warm app until #555's conversion lands, and a converted lease whose first renewal was not answered) must wait for its raise to be replicated (ADR 0268, as grant_app_lease reads it: a successor counts a gate only for a row that reads expiring). But grant_pending, grant_waiting_on and in_progress cover only a marked row, so a second failover during that wait is not refused and the wait is not shown. The maintainer chose this: the failover's write sets the unsettled mark whenever its grant must wait for replication, so ADR 0292 and ADR 0295 cover it, and the settle at the first renewal after the expiring grant clears it. Alternative: such a failover answers lease_held while the row reads local, as for a local lease. Q3. A connected source: after its revoke is answered, the new token still waits for the lease's gate (section 8.4, 'What the authority must enforce'): up to D + 2M, 18 s at D = 15 s. The reading of 'goes on once the revoke is answered' is that the call stays open through that bounded wait and then commits. If support lapses meanwhile, the call answers no_support and the source stays revoked and stopped (safe: no second copy runs). A retry goes straight to the grant because the revoke is already answered. The maintainer confirmed both.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md): which writes set the mark: also a failover's write whose grant must wait for replication, a raise of duration_ms included. The rest of [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md) stands.
- Refines [ADR 0292](0292-failoverapp-answers-202-at-once-while-the-grant-of-a-marked.md): which failovers wait under the mark, and how long failoverApp stays open for a connected source: through the gate wait after the revoke is answered, then it commits or answers no_support. The rest of [ADR 0292](0292-failoverapp-answers-202-at-once-while-the-grant-of-a-marked.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0303 in the project decision ledger. Do not edit by hand; changes go through a new decision.
