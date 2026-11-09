# ADR 0299: A node's hello counts toward a marked row's grant only once that session's revokes are answered, and step 4 revokes first a lease listed in the other mode

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0299
- **Tags:** m5b, leases, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md), [ADR 0295](0295-a-restarted-primary-sends-a-marked-row-s-pending-new-token.md)

## Context

Found by the independent review of the contract change for D-0295. Step 4 of 'The holder fenced first' now sends a marked row's new-token grant to a node that does not list the lease. A node that lost node.db lists nothing too, and a successor primary that lacks an unmarked move can then send it the grant while the node that took the move still runs the app, because a hello.log_seq counts as an acknowledgement before the primary's revoke of that other node is answered. The same review found that a node listing the lease in the other mode at a lower token refuses step 4's grant and gets no revoke.

## Decision

A node's hello counts toward a marked row's grant only once that session's revokes are answered, and step 4 revokes first a lease listed in the other mode.

### 1. A node that lost its store can get back a lease that moved

D-0295 lets step 4 of "The holder fenced first" (node-agent-protocol.md section 8.4) send a marked row's new-token grant to a node that does not list the lease. Before D-0295, a node with no listing got nothing back (D-0281). A node lists nothing in two cases: it never took the token, or it lost `node.db`, which section 7.1 names as a case (the `leases` table lives in `node.db`). With the second case, the grant can go out while another node runs the app.

**Sequence.** Nodes N and N2, primaries P1 and P2.

1. A marked write names N `local` at token t. It is replicated, and N takes the grant.
2. N reports `local`, and P1 clears the mark. The app's policy then leaves `warm`.
3. A restore moves the lease to N2 at t+1. The row is not marked, so N answers its revoke and N2's grant goes out at once.
4. P1 dies. P2 is promoted from a log that lacks the clear and the restore. These are unmarked writes, so nothing waited for them to be replicated. N loses `node.db`.
5. P2's row is marked and names N `local` at t. N's `hello` lists nothing, so step 4 sends N the grant at t once the row's newest write is replicated over the machines with the `node` role and P2 holds majority support.
6. N2's `hello` lists the lease `held` `local` at t+1, and its `log_seq` matches P2's log. P2 revokes N2 and raises the marked row's token (section 8.4, "The mark"). But a `hello.log_seq` counts as an acknowledgement as soon as its hash check matches (section 8.4, "When R is replicated", which "Grants of a marked row" uses), before that revoke is answered.
7. If N's session starts in that window, N gets t while N2 still runs t+1 `local`: two nodes run the app.

Once the raise is committed and is the row's newest write, N2 must acknowledge it, and section 4.2 rule 3 holds N2's `log_ack` back until it has answered the revoke. So the gap is only the `hello` acknowledgement before the revoke is answered.

**Options.**

- **(a), which the maintainer chose.** A node's `hello.log_seq` counts toward "Grants of a marked row" only once the node has answered every `lease_revoke` the primary sent at that session start, and every token raise those revokes cause is committed. This follows the pattern of ADR 0239 for `fence_ack`. A later `log_ack` needs no new rule, because section 4.2 rule 3 already holds it back until the revokes are answered. Also:
  - step 4 checks its conditions again when it sends the grant;
  - the "Not listed" bullet keeps its present reason, "the node lists no token for the lease";
  - T-LEASE-16 names a node that lost `node.db` among its risks.

  Cost: a node counts a little later at each session start. No wire change, no new field and no migration.
- **(b)** The node keeps its highest token per lease outside `node.db`, next to its mark (section 12), and lists it in `hello`. Then a node that lost its store still reports the token it held, and "not listed" means it never held one. Cost: a new node-side file, a `hello` change, and a migration path for existing nodes.
- **(c)** Drop the not-listed case from D-0295 and keep "not listed: nothing back". A failover's or a restore's held-back grant then still stalls after a primary restart, because the node the row names never held the lease. That is the gap D-0295 closed.

### 2. A node that lists the lease in the other mode refuses step 4's grant

Step 4 sends a grant whose mode can differ from the lease the node lists. A node that lists the lease `held` or `uncertain` at a lower token in the other mode refuses a higher-token grant of that mode with `bad_message` (section 5.12, ADR 0258). No revoke follows, so a `local` holder runs on with no deadline, and the lease never settles. While a node holds a lease, the primary gives it a new token only in the mode it holds it in, so only a node store that went back reaches this case. It runs no second copy, because the row still names that node. The contract change for D-0295 states it as a limit.

**Options.**

- **(i), which the maintainer chose.** In that case the primary first revokes at the token the node reports, then sends the grant. The node holds the grant back until it has answered the revoke (section 4.2 rule 3), as section 8.5 already does for a lease listed `expired` at a lower token. Cost: one more case in step 4; the app restarts once on the same node.
- **(ii)** Keep the stated limit.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md): when a hello.log_seq counts toward a marked row's grant: only once the node has answered every lease_revoke sent at that session start and every token raise they cause is committed. The rest of [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md) stands.
- Refines [ADR 0295](0295-a-restarted-primary-sends-a-marked-row-s-pending-new-token.md): what step 4 does when it sends: it checks its conditions again, and a node that lists the lease in the other mode at a lower token is revoked at that token first. The rest of [ADR 0295](0295-a-restarted-primary-sends-a-marked-row-s-pending-new-token.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0299 in the project decision ledger. Do not edit by hand; changes go through a new decision.
