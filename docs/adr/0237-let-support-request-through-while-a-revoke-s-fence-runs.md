# ADR 0237: Let support_request through while a revoke's fence runs, like renewal answers (extends ADR 0222)

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0237
- **Tags:** m5b, contracts, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0222](0222-rule-3-of-section-4-2-lets-renewal-answers-for-other-leases.md)

## Context

ADR 0222 lets lease_grant for other leases and heartbeat/ping through while a revoke's fence runs, and holds back every other message. On a voter that also holds back support_request (S = 15 s, asked every S/3 = 5 s). A local fence with no deadline can then let this voter's support lapse on the primary, and with one other voter down the primary loses its majority and stops renewing every expiring lease in the cluster: the same needless-outage pattern ADR 0222 fixes, one level up.

## Decision

Let support_request through while a revoke's fence runs, like renewal answers (extends ADR 0222).

Add support_request to the messages a node processes while a revoke's self-fence runs (section 4.2 rule 3, section 5.13), and let it answer with support_grant. It is safe for the same reason as ADR 0222: a support promise starts no VM and changes no lease on this node, and the per-lease start gate keeps the revoked lease's VMs stopped. The alternative, not chosen, kept ADR 0222 literal and accepted that a long local fence on a voter can cost the primary one voter's support for up to the fence's length. The maintainer accepted letting support_request through on 2026-11-06. The contract change for ADR 0222 kept that decision literal; this one extends it.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0222](0222-rule-3-of-section-4-2-lets-renewal-answers-for-other-leases.md): the messages a node processes while a revoke's fence runs (on a voter, support_request as well). The rest of [ADR 0222](0222-rule-3-of-section-4-2-lets-renewal-answers-for-other-leases.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0237 in the project decision ledger. Do not edit by hand; changes go through a new decision.
