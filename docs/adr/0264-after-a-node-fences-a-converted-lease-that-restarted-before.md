# ADR 0264: After a node fences a converted lease that restarted before its first renewal, the primary regrants it from hello at a higher token

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0264
- **Tags:** m5b, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

A lease converted from local to expiring (ADR 0241) has a provisional deadline only once its first renewal is sent, and the node stores that deadline only after the send. If the node agent restarts before then, the lease has no stored deadline, so it comes back expired and is fenced (section 8.4, Agent restart): the node cannot tell that no renewal left. The primary's row is still local, because no renewal reached it (ADR 0244). The node's next hello lists the lease as expiring and expired at the same token. A same-token grant cannot revive an expired lease (section 5.12), and nothing on the primary regrants a lease it believes the node holds, so the app stays down until an operator acts. The same holds whenever a node loses a lease the primary still gives it.

## Decision

After a node fences a converted lease that restarted before its first renewal, the primary regrants it from hello at a higher token.

When a node's hello lists as expired or revoked a lease that the primary's row gives that node, and the node reports no failed fence for it, the primary grants the same node a strictly higher token with the mode the app's policy wants. Rejected: B (the node writes its t_send reading ahead of the first renewal) and C (nothing automatic). Contract change first (sections 8.4, 8.5), then the primary's implementation.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0264 in the project decision ledger. Do not edit by hand; changes go through a new decision.
