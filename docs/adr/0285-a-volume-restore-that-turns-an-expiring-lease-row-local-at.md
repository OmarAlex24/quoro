# ADR 0285: A volume restore that turns an expiring lease row local at a new token sets the unsettled mark as R does, so its grant waits for replication and majority support

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0285
- **Tags:** m5b, leases, contracts, review
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md)

## Context

A second independent review of the contract change for D-0261 with the D-0280 fixes found a new Blocker. A volume restore of a warm app revokes the expiring holder and grants the target local at a new token on an unmarked row, with no replication and no support, so a successor that lacks those entries fails the app over to its warm copy while the target runs it local. The core rule and the earlier fixes held; one Major and eleven Minors are mechanical.

## Decision

A volume restore that turns an expiring lease row local at a new token sets the unsettled mark as R does, so its grant waits for replication and majority support.

### How this was found

A second independent review covered the contract change for D-0261 with the fixes of D-0280. The reviewer received three things: the contract diff against main, the text of the design, and the earlier review. It could also read the contract files on the branch.

**The core rule held against every sequence the reviewer built.** That covers:

- R, written first and replicated over every machine with the `node` role, under majority support;
- the required set, across joins and removals;
- the freeze;
- the persisted mark.

**Earlier findings.** Every earlier finding is closed, with two exceptions:

- The placement Blocker is closed for marked rows. The Blocker below is its unmarked twin.
- The fix of the regrant on `revoked` holds for the regrant from `hello`, but a side door reopens it (Major below).

### Blocker: a volume restore of a `warm` app grants `local` at a new token on an unmarked row

**Text.**

- Section 8.3, "Granted at placement": "A restore revokes the lease on the old node with reason `moved` and, once the new volume is `ready`, grants a new one to the target node with a strictly higher token".
- `restoreBackup`: "the primary grants the target node a new `local` lease with a higher fencing token".
- "Grants of a marked row": "Grants of an unmarked row are unchanged: a `local` one needs no support, as before."
- Nothing refuses `restoreBackup` for an app whose policy is `warm`. Its `409` list covers:
  - a backup that did not succeed or was pruned;
  - the control-plane database;
  - a disconnected current node;
  - a restore in progress;
  - a target with a failed fence.

**Sequence.**

1. The machines are nodes A (the primary's host), H, D and X, plus a witness. App P is `warm`. Its row reads `expiring`, H at token 5, unmarked, and D is P's warm node.
2. D goes down.
3. An operator restores a backup of P onto X. A revokes H at 5, and H fences. When the volume is ready, A grants X `local` at 6, with no replication and no support. X runs P with no deadline.
4. A dies. D comes back, with a log that ends before step 3, and is promoted without force.
5. D sees P `warm`, its row `expiring` for H at 5, and no X.
6. A failover onto D's warm copy waits for D's start gate, then starts P at token 6, while X, cut off, runs P `local` at 6.

The contract names this hazard itself in "Ending the renewals": "A new token does not avoid it either, because a successor that lacks the revoke and the new grant sees the old `expiring` lease". Only R and a control-plane restore set the mark, and a volume restore that turns an `expiring` row `local` at a new token is neither. The new T-LEASE-01 sentence overclaims.

**Options.**

- **(a) Recommended.** Any write that turns a row that reads `expiring` `local`, at any token, sets the unsettled mark as R does. A restore's write onto a `warm` app's row is the case at hand. Its grant then falls under "Grants of a marked row": the restore is not refused, and its grant waits until every machine with the `node` role holds the write and the primary holds majority support. This reuses the rule already chosen for marked rows, and it covers any later path that turns an `expiring` row `local`.
- **(b)** `restoreBackup` answers `409 conflict` while the app's policy is `warm`. The operator leaves `warm` first, which runs R, and restores after. This is simple, but it closes only this one path.
- **(c)** A restore grants in the mode the policy asks for, `expiring` for a `warm` app, so the target fences on its own deadline. This changes the restore semantics of ADR 0147 for `warm` apps.

Also, state the token at which a primary revokes a non-holder that lists a lease of an unmarked row: the token the node reports, as for a marked row.

### Major: the raise of a marked row's token reopens the regrant on `revoked`

**Text.**

- "The mark": "When a node that a marked row does not name reports the lease, the primary revokes it at the token the node reports ... It then raises the row's token above that, keeping the mark."
- Step 4 of "The holder fenced first" sends the grant instead of the revoke for "a marked row whose `revoked_at` is null".

A row raised this way has every property step 4 tests. So a later primary can send the old holder a `local` grant at the raised token, although that holder listed the lease `revoked`. The sequence needs a node enrolled in entries the successor lacks, and a reading of "reports the lease" that includes a `revoked` listing.

**Fix (mechanical, within ADR 0280):**

- the raise also sets `revoked_at`, as declaring a node lost does, so step 4 never sends it;
- "reports the lease" means lists it `held` or `uncertain`;
- state as a limit a node enrolled in entries the successor lacks.

### Minors (mechanical, within ADR 0280)

1. Say what the statement for `quoro cluster promote --accept-loss` asserts.
2. The regrant from `hello` takes a token above both the row's token and the one the node reports.
3. In "The holder's next `hello`", a `local` or `expiring` report means the lease is `held` or `uncertain`.
4. T-LEASE-16 also lists the settle among the writes that clear the mark.
5. `failoverApp` and section 8.4 say the same thing about when a marked lease answers `lease_held`.
6. `cluster.restored.params.term` names the term of the first write, which a restart above a `stale_term` may raise.
7. At start, a mark at 2^53 - 1 with no refollowing state means "refollowing, head unknown".
8. Specify the restored primary's first write further:
   - how the primary detects its first start after a restore, through a marker file that the first write's commit consumes;
   - how it learns of `--accept-loss` and the statement;
   - that the write is safe to repeat after a crash.
9. A failover of a marked row whose holder is declared lost waits until that holder is removed or reports, because a lost node stays in the required set. State it, and that `failoverApp` answers once the grant is sent.
10. After a new-token `local` grant, the mark and `local_pending` stay until the node's next session. State it.
11. `--abort` after the new database is written: the operator moves that database aside before a rerun.

## Consequences

- Refines [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md): which writes set the mark and what a raise records: any write that turns a row reading expiring into local, at any token, sets the mark, a volume restore's included; the raise of a marked row's token also sets revoked_at; and the side paths listed here are fixed or stated as limits. The rest of [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0285 in the project decision ledger. Do not edit by hand; changes go through a new decision.
