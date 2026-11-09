# ADR 0280: Every grant of a marked lease waits until every node holds the row's write, the mark is kept in the row, the frozen log is checked again, and removals count only machines not removed (completes ADR 0261)

- **Status:** Accepted; refined by [ADR 0285](0285-a-volume-restore-that-turns-an-expiring-lease-row-local-at.md), [ADR 0295](0295-a-restarted-primary-sends-a-marked-row-s-pending-new-token.md), [ADR 0303](0303-failoverapp-before-the-primary-can-send-a-held-grant-what-a.md)
- **Date:** 2026-11-07
- **Ledger id:** D-0280
- **Tags:** m5b, leases, contracts, review
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md), [ADR 0285](0285-a-volume-restore-that-turns-an-expiring-lease-row-local-at.md), [ADR 0295](0295-a-restarted-primary-sends-a-marked-row-s-pending-new-token.md), [ADR 0303](0303-failoverapp-before-the-primary-can-send-a-held-grant-what-a.md)

## Context

An independent review of the contract change for D-0261 found five gaps in its side paths, none in its core rule. A placement grant during R's wait, and the hello regrant on revoked (ADR 0264), can each let a successor that lacks a move start a second copy; a row R wrote is not marked; the log refusals run before the freeze; two pending removals keep each other in the required set.

## Decision

Every grant of a marked lease waits until every node holds the row's write, the mark is kept in the row, the frozen log is checked again, and removals count only machines not removed (completes ADR 0261).

### How this was found

An independent review of the contract change for D-0261. The reviewer received:

- the contract diff;
- the text of D-0261;
- read access to the contract files on the branch.

It got nothing under `docs/adr/` and none of the writer's reasoning. Its brief was to hunt for double runs and for a sealed candidate that lacks R.

It found the core rule sound: R is written first and the `local` grant is sent only once every machine with the `node` role has acknowledged R, under majority support. Every problem below is in a side path.

The event sequences are the reviewer's. These were checked against the text:

- every quoted sentence exists;
- the regrant on `revoked` is the text of ADR 0264;
- `restoreBackup` lists no refusal for a lease whose `local` write is waiting;
- both cases that motivate ADR 0264 (an agent restart before a converted lease's first renewal was sent, and a deadline that passed while the session was down) come back `expired`, not `revoked` (section 8.4, "Agent restart").

### A placement grant moves the lease while R waits

**Text.** Section 8.4: "The lease moves only as a `local` lease does: through a revoke its holder answered, a fence acknowledgement, or a node declared lost", and "Placement grants need none, as before". `restoreBackup` revokes the holder's lease and grants the target a new `local` lease with no replication and no support.

**Sequence.**

1. Nodes A (the primary's host), H (the holder) and D (the warm node), plus a witness.
2. D goes down. The app leaves `warm`, and R (row `local`, H, token 5) waits on D.
3. An operator restores a backup onto node X. H answers the revoke, and X gets `local` at token 6.
4. A dies. D comes back and is promoted. It passes every refusal and needs no `--accept-loss`.
5. D sees the policy `warm`, the row `expiring` for H at token 5, its own warm copy, and no X.
6. A failover onto D's own warm copy, once the gate passes, starts a second copy. A regrant to H from `hello` also starts one. Meanwhile X, cut off, keeps running `local` at token 6.

**Fix.** While R, or a regrant's own write, is not replicated, give every new-token grant of that lease R's treatment: write it first, and send it only once that write is replicated over the required set and while the primary holds majority support. The alternative is to refuse, with `409 conflict`, every operation that grants the lease to another node while `local_pending` holds. Declaring a node lost and removing a node grant nothing, so they stay as they are.

### The regrant from `hello` on `revoked` hands back a lease that was moved

This is in ADR 0264's text, which D-0261 widens to an unlisted lease.

**Text.** Section 8.5: "The primary therefore gives it back when a node's `hello` lists a lease as `expired` or `revoked` and:" the row names the node, `revoked_at` is null, and no failed fence is reported. D-0261 adds: "The primary gives the lease back to that same node, so no second node runs the app".

**Sequence.** No R is involved.

1. A `none` app runs on H at token 5.
2. Node C is down. A restore moves the app to X: H answers the revoke at 5, and X gets `local` at 6.
3. The primary dies. C, which lacks those entries, is promoted without force.
4. H lists the lease `revoked` at 5, and C's row says `local`, H, 5, `revoked_at` null.
5. C regrants H `local` at 6 at once, with no gate, no support and no replication.
6. H starts on its detached volume while X, cut off, runs `local` at 6.

The rule cannot tell "the old primary revoked it and the move's grant is in entries I lack" from "the old primary crashed between the revoke and the grant". A revoke for a move carries the current token, so the token does not tell them apart either.

**Fix.**

- Regrant only on `expired` and on a refused same-token `local` grant. Both are deadline expiries, and both cases that motivate ADR 0264 come back `expired`, so this costs no liveness.
- Never regrant on `revoked` at or above the row's token. Keep the revoke of a lease listed below the row's token (section 5.1).
- For a lease the holder does not list, require the R-style replication, and state as a limit a node enrolled in entries the successor lacks.
- Define what the statement for `quoro cluster promote --accept-loss` asserts. Unsettled rows also reach this path when the holder reports `revoked` or nothing.

ADR 0281, which supersedes ADR 0264, decides it.

### Nothing persisted marks a row that R wrote

**Text.** "for a row that R wrote", section 8.5's "For a row that the end of an `expiring` lease's renewals wrote `local`", and "the primary cannot tell the two apart". `local_pending` is not stored. A restarted primary cannot tell an R row from a placement row, nor find R's `seq` to check its replication.

**Sequence.**

1. D is down, so R waits.
2. The witness blips, the primary loses support, and H fences at `valid_until`.
3. The primary restarts. H reports `expired` at 5, and the row looks like a placement row. The ADR 0264 path regrants `local` at 6 at once.
4. A dies, and D, which lacks R and the regrant, is promoted and sees `expiring` for H at 5.
5. H reports `held` `local` at 6, which no rule covers for a row that is `expiring` and not unsettled.
6. If H is cut off, D's gate passes, and a failover onto D's copy is a double run.

**Fix.** Persist the mark in the row. R could set `leases.unsettled`, or a new column, that only the write following the holder's `local` report clears. While it is set, every grant of the lease gets R's treatment. Also say what a `local` report at a token above an `expiring` row does. This changes migration 0013 and the resend and regrant rules.

### The log checks run before the freeze, and step 4 only compares with the frozen log

**Text.** Section 7.3: the refusals run "Before it changes anything", and step 4 "builds from exactly the frozen log: it checks that the freeze held, and stops if the log has changed".

**Sequence.**

1. The candidate passes its refusals with a log past R.
2. Before the freeze, its still-following agent is told `log_diverged` by a briefly serving primary. It quarantines, lowers its mark to 0 and refollows part of the log. A corrupted `node.db` plus an agent restart does the same.
3. The candidate then freezes the short log, and step 4 builds without R.

**Fix.** In step 4, re-run every log refusal (empty, quarantined, below the mark, refollowing) against the frozen log.

### Two pending removals keep each other in the required set

**Text.** "A removed machine leaves the set only once its removal entry is itself replicated over the remaining machines of the set; until then it still counts".

**Sequence.** Two dead nodes, D1 and D2, are removed one after the other. Each one's "remaining machines" include the other, which never acknowledges, so neither leaves. No app can leave `warm` again, and no command undoes a removal. This contradicts "No setup is stuck for good".

**Fix (wording).** "replicated over the machines of the set that are not removed". The reviewer checked that this stays safe for two removals in a row.

### Smaller gaps

- **The refollowing guard and the mark.** The guard lives in `node.db`, and the mark drops to 0 at the quarantine. The order inside "the same step" is not stated. Write a value above any `seq` to the mark at the quarantine, then the `head_seq` once it is known.
- **A primary host reverted to a VM snapshot** comes back at the same term with a shorter log, and its nodes refollow a history without R. Say this is unsupported, or refollow only from a higher term.
- **The term from `check` answers.** Learning a higher term needs a second `check` at the raised term, and a rerun of the command should start from what it learned. Say so.
- **The freeze has no owner** after a killed command or a reboot, and a frozen agent holds back every R. Add a way to end it.
- **A return to `warm` on a live session while R waits** is not covered. Apply the settle there too.
- **"or right after it".** A crash between the policy write and R leaves an `expiring` row under a policy off `warm`. Require one transaction, or a sweep at start.
- **A restore with `--accept-loss`** also breaks section 8.10 rule 1 ("exactly one voter"). A fresh host with no node of its own removes every node.
- **Regrant details.**
   - A restarted primary has no gate for a `local` R row in step 1.
   - The regrant waits on a node that is down, and that cost is not stated.
   - Step 4 at a session start without support sends neither a revoke nor a grant.
- **Cross-references.**
   - `failoverApp`'s `lease_held` covers only a lease "still `local`", yet an unsettled row reads `expiring`.
   - `Lease` has no `unsettled` field.
   - No new-token write clears the mark.
   - `local_pending` cannot be known after a restart.
- **An equal-term restore** gets a `bad_message` without a term from a voter that voted in that term.
- **Two writes at first start:** the first write and `cluster.primary_moved`. Say which comes first, or merge them.
- `cluster.restored` has channel `cli`, but the control plane writes it at its first start; `cluster.primary_moved` uses `system`.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md): which grants wait and what the mark covers: while a lease row is marked, every grant of the lease, a new token's included, waits until every node holds the row's newest write; R sets the mark and only the holder's report clears it; the frozen log is checked again; a removal counts only machines not removed; and the side paths listed here are fixed or stated as limits. The rest of [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md) stands.
- [ADR 0285](0285-a-volume-restore-that-turns-an-expiring-lease-row-local-at.md) refines which writes set the mark and what a raise records: any write that turns a row reading expiring into local, at any token, sets the mark, a volume restore's included; the raise of a marked row's token also sets revoked_at; and the side paths listed here are fixed or stated as limits. The rest of this decision stands.
- [ADR 0295](0295-a-restarted-primary-sends-a-marked-row-s-pending-new-token.md) refines which marked rows step 4 of 'The holder fenced first' sends a grant for: any marked row whose newest write names the node as holder at a higher token than the node reports, in either mode, or a lease the node does not list. The rest of this decision stands.
- [ADR 0303](0303-failoverapp-before-the-primary-can-send-a-held-grant-what-a.md) refines which writes set the mark: also a failover's write whose grant must wait for replication, a raise of duration_ms included. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0280 in the project decision ledger. Do not edit by hand; changes go through a new decision.
