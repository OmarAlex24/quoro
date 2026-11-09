# ADR 0261: Every node holds a lease row's local write before an expiring lease's renewals end (completes ADR 0255 and ADR 0256 part 2)

- **Status:** Accepted; refined by [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md)
- **Date:** 2026-11-07
- **Ledger id:** D-0261
- **Tags:** m5b, leases, contracts, election, warm
- **Supersedes:** an internal decision (not published)
- **Superseded by:** none
- **Related:** [ADR 0255](0255-a-conversion-of-an-expiring-lease-to-local-is-sent-only.md), [ADR 0256](0256-an-expiring-grant-that-carries-re-never-converts-a-lease.md), [ADR 0179](0179-change-log-follow-74-every-machine-with-role-node-follows.md), [ADR 0227](0227-change-log-follow-sends-one-log-entries-batch-at-a-time-and.md), [ADR 0182](0182-manual-promotion-19-quoro-cluster-promote-on-the-candidate.md), [ADR 0232](0232-quoro-cluster-promote-refuses-on-a-host-that-already-has-a.md), [ADR 0148](0148-control-plane-backup-vacuum-into-on-the-primary-every-6-h.md), [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md)

## Context

Revises an earlier design after a fourth independent review, which found five safety and three liveness problems in its side paths and none in its central rule. Main change: the lease row is written local first and the grant is sent only once that write is acknowledged by every node-role machine, which is D-0255's own sentence; 'possibly local' by entry order is gone and survives only as a persisted unsettled mark set by every restore and forced recovery. Also: a removal shrinks the required set only once it is itself replicated; the hello regrant is limited to rows it wrote; a forced restore removes the dead nodes; the node's mark is reset on a quarantine and acknowledgements reset on log_diverged; the candidate takes its term from vote answers; the settle waits for a pong. Corrects the earlier design's claim that removing a node below three voters needs force (section 10 says it does not). The fourth pass's fixes were not yet reviewed when this was decided.

## Decision

Every node holds a lease row's local write before an expiring lease's renewals end (completes ADR 0255 and ADR 0256 part 2).

### Problem

D-0255 sends the same-token `local` grant that ends an `expiring` lease's renewals only once the HA-policy change is "committed in the replicated change log on a majority of the voters", "until the lease row is local and replicated". It says a successor primary therefore sees the lease as `local`. The contract does not give that, for three reasons:

- **No log check in the vote.** Section 8.9's voter rules check the term, the promise and `voters_version`. The vote carries no log position, so a majority does not imply that the winner holds the write.
- **Promotion loses unreceived entries.** Section 7.3 step 1: "Entries the old primary wrote and this machine never received are lost to the cluster". This comes from #405, ADR 0182 ("checks its log is current to its last ack and shows the lag") and ADR 0232 (`--dry-run` stops after the log position).
- **Witnesses hold no log.** Section 7.1: only machines with the `node` role follow the log, so a witness never acknowledges anything.

What it costs: a successor that lacks the write sees the row `expiring`. Once the gate passes, it starts the app on another node while the holder keeps running it `local`, which is a double run.

A new token does not avoid it. The alternative path is to revoke, fence, and grant `local` at token + 1. A successor that lacks those entries sees the old `expiring` lease and issues the next token elsewhere.

### Recommended design: every node holds the row's `local` write before the grant

#### 1. The rule

**R is the write that ends the lease's renewals.** It writes the lease row `local`, in the same transaction as the HA-policy change off `warm` or right after it.

**R is replicated** when every machine of the *required set* has acknowledged a seq at or above R's in the primary's log.

- **What counts as an acknowledgement.** A `log_ack` on the session, or a `hello.log_seq` once section 7.1's hash check has matched it. The primary's own host counts through its own agent, like any node. An acknowledgement no longer counts once the primary declares that node's log diverged (section 1.3).
- **The required set.** Every enrolled machine with the `node` role, counted from its enrollment entry.
- **A removed machine** leaves the set only once its removal entry is itself replicated over the remaining machines of the set. Until then it still counts. So every later primary holds the removal: the removed machine's credential is revoked and its seals are deleted there.

**Why every later primary holds R:**

- **Only sealed machines are promoted.** A promotion runs only on a machine with the `node` role whose custody is `sealed` (section 7.3).
- **Members at R's time.** A machine that was in the required set when R was replicated acknowledged R. It keeps R (section 1.3). If it diverges and refollows, promotion refuses it until it holds R again (section 1.3).
- **Machines that join later.** A machine that gets the `node` role later is sealed only after it reports a custody key. The primary writes those seals to the log when it first stores that key (section 7.2, "When"), so they come after R. The log is hash-chained from seq 1, so a machine that holds its seals holds R.
- **A removed machine cannot be elected.**
  - Every later primary holds its removal, and it is no voter of the newer voter set. The listener refuses it (section 8.9).
  - It cannot win a majority of the older set either. The `local` grant needs majority support at the primary's current version (section 8.10 rule 3 and section 2 below). Voters that gave that support never vote at an older version. A majority of the older set includes one of them, because the two sets differ by one voter.
- **Others are never candidates.** Witnesses and edge-only machines are never candidates (section 8.9, "Listener").

The rule needs no change to `POST /v1/vote` and no majority. A witness-heavy voter set and the term of a promotion do not affect it.

**Setups:**

| Setup | R is replicated when |
| --- | --- |
| Demo: node A (the primary's host), node B, witness W | A and B have acknowledged |
| Two nodes, no witness | Both have acknowledged |
| Three nodes, with or without witnesses | All three have acknowledged |
| One node A and one or more witnesses | A has acknowledged |
| One node, no other voter | That node has acknowledged |

- **Non-voter replicas.** From M5b every machine with the `node` role is a voter (section 6.12), so there are none. A stateless replica runs on a node and counts like any other node.
- **A primary host with no `node` role.** It is not counted; it holds R in its own database.

#### 1.2 The candidate builds from the log it acknowledged

**Freezing the agent.** Before any real vote request (not `check`), `quoro cluster promote` freezes its own agent through a local call. The freeze is stored in the agent's local store (section 12), so it survives an agent restart. While it lasts, the agent:
- ends its session with the primary;
- does not redial (section 4.4);
- stores no more `log_entries`;
- keeps its log as it stands.

The freeze does not depend on the agent's own vote, which a primary the agent still supports may make it refuse.

**Building.** Step 4 builds the database from exactly the frozen log. It checks that the freeze held, and stops if the log has changed.

**The term.** The candidate asks for one above the highest term it has seen or that any vote answer carries, `check` answers included. That term is used by the CLI only; the agent stores a term only when it grants a real vote. Without this, a promotion that stopped after its real votes would leave voters at t + 1, and they would refuse every other candidate that asks for t + 1 with `stale_term`.

**If the promotion stops:**
- If its own agent did not grant the real vote, the agent unfreezes, dials again and resumes following.
- If its own agent granted it, the agent is at term t + 1. It answers the old primary's `welcome` with `stale_term`, which demotes that primary (section 8.8). The promotion must then be finished or retried at term t + 1 or above. Section 7.3's `check` before the real vote keeps this case rare.

**Why.** Without the freeze, the agent could acknowledge R to the old primary after the candidate read its log. The old primary would then count R replicated while the new database lacks it.

#### 1.3 Durable logs, a log that went back, and refollowing

**Durability.** A node writes each verified entry durably (fsync) before it sends `log_ack`. It reports in `hello.log_seq` only what is durable.

**A log that went back.** A node's log can lose entries while its identity and custody key survive, because section 12 keeps those outside the SQLite file. That happens when `node.db` is deleted after corruption, or when the host is rolled back to a disk snapshot.
- **The node's mark.** The node keeps a high-water mark of its acknowledged seq next to `custody.key`. It fsyncs the mark before the `log_ack` or `hello` that reports it. `quoro cluster promote` refuses when the log is below the mark.
- **The primary's check.** If a `hello` reports a `log_seq` below the newest acknowledgement the primary holds for that node, in memory or in `nodes.log_seq`, the primary answers `log_diverged` and the node refollows.
- **What neither catches:**
  - A whole-host rollback, which takes the mark back with it.
  - A primary that crashed since the node's session began. `nodes.log_seq` is written only at session start and end (ADR 0227), so the primary lost the acknowledgements in between, and the node's mark is then the only guard.

  Both are listed under "Costs".

**Refollowing.**
- **When it starts.** A node whose log the primary declares diverged quarantines that log and refollows from seq 1 (section 7.1).
- **When it ends.** It is refollowing until its new log reaches the `head_seq` of the first `log_entries` message it receives after the quarantine. That primary holds every replicated entry, so the node then holds them all again.
- **Recording it.** The node records "refollowing, head unknown" in the same step as the quarantine, and lowers its mark to 0. It fills in the head_seq before it stores any new entry. The state survives an agent crash at any point.
- **What the primary resets.** On `log_diverged` the primary sets its in-memory acknowledgement for that node and `nodes.log_seq` to 0. Otherwise the next `hello` would read as a log that went back.
- **Promotion meanwhile.** `quoro cluster promote` on that node refuses, as it does for a quarantined log.

**Way out.** `quoro cluster promote --accept-loss` lifts these refusals when no other candidate is left. It builds from the current log only, and every entry past that log's head is lost, replicated or not. It needs `--statement`, and one statement serves `--ack-fence` too when both flags are given. The record keeps the statement as its `justification` and sets `cluster.promoted.params.accept_loss`. Section 4 says what follows.

#### 2. What the primary does (folds in the readings)

**Writing R and sending the grant.**
- When the app's HA policy moves off `warm`, the primary writes R, the lease row as `local`.
- Once R is replicated (section 1), and while it holds majority support, it sends the same-token `local` grant, followed by the ping of section 8.4.
- This is D-0255's "Until the lease row is local and replicated, the lease stays expiring and keeps renewing", with "replicated" meaning section 1.

**While the row is `local` and the holder may still be `expiring`.** That is, from R until the holder has taken the grant.
- **Renewals.** The primary answers the holder's renewals under majority support (section 8.8), as for an `expiring` lease, until it sends the `local` grant on the session. This is the reading of ADR 0256 part 2: "a lease whose row it has made local" means from the moment the primary sends the `local` grant, and from then on it answers no renewal of the lease on that session.
- **Moving the lease.** The lease moves only as a `local` lease does: through a revoke its holder answered, a fence acknowledgement, or a node declared lost (sections 8.3 and 8.5). `failoverApp` answers `lease_held`, as D-0255 promises. A `local` row never moves on a gate, so a restart that loses the gates in memory loses nothing.
- **The holder's next `hello`.** It tells the primary which case it is in:
  - **`local` with the row's token:** done.
  - **`expiring` with the row's token, and the policy still off `warm`:** the primary sends the `local` grant again, once R is replicated and it holds majority support. This is D-0255's resend. Until now nothing re-sent a `local` grant lost with a session. A restarted or promoted primary does the same.
  - **`expiring` with the row's token, and the policy back on `warm`:** the primary settles the lease (below) and writes the row `expiring`.
- **No conversion grant without the mode.** The primary sends no conversion grant to a holder whose mode it has not learned on the current session. A same-token `expiring` grant without `re` over an `expiring` lease has no row in section 8.4's table, and this design never sends one. For a holder that reports `local`, a revert to `warm` uses the conversion of section 8.4 (ADR 0244 and ADR 0254).

**Settling a lease.** The primary answers a renewal of the holder on the current session, then sends a `ping`. Once the `pong` arrives, it writes the row with the token and the longest `duration_ms` it granted on that session.
- **Why the pong.** The answer replaces the node's deadline with one computed from this primary's grant (section 8.4's table: "Recompute `valid_until` from the new `t_send`"), but only once the node has received it. The `pong` to a `ping` sent after the answer proves that it has.
- **What it guards against.** Without it, a node that an earlier primary had granted a longer duration, such as a maintenance window's `W`, could run past a gate counted at `D`.

**Writing the row `local`.** A holder whose `hello` reports `local` at the row's token while the row is `expiring` always gets the row written `local`.

**The holder fenced first.** The holder may report the lease `expired` or `revoked`, or not list it in `hello`. It may also refuse the same-token grant because `valid_until` passed while the grant was in flight (section 5.12). Either way the node holds no lease for the app, whatever its VMs are doing, and the primary gives the lease back to that same node, so no second node runs the app. The primary:
1. waits until the lease's gate has passed, as section 8.4 requires before it gives the same node a new token after an expiry;
2. writes the row `local` with the next token, as at placement;
3. sends the grant once that write is replicated and the primary holds majority support;
4. at each later session start where this row names the node as `local` at a higher token than the node reports, sends that grant instead of section 5.1's revoke. This applies only to a row that step 2 wrote: `revoked_at` is null, and the lease is in neither `fence_failure` nor `fence_ack_pending`. A row that a node declared lost or a fence acknowledgement wrote keeps its revoke (sections 8.3 and 8.5).

**Majority support carries part of the argument.**
- **The rule.** Every `local` grant this design sends needs majority support: the grant itself, a resend and a regrant. This narrows section 8.8's "`local` leases need no support" for these grants only. Placement grants are unchanged.
- **Why it matters.** Under section 8.8's promise, no voter of that majority can meanwhile have voted for a higher term. So the grant never races a later primary.

**Readings folded in:**
- **ADR 0257's barrier after a session cut.** It is the holder's next `hello`, which stands in for the `pong`, since no renewal of the old session can arrive on the new one. ADR 0244's rule ("when the holder's `hello` reports the lease as `local`, send the conversion grant again") then applies.
- **ADR 0256 part 2 and ADR 0244.** ADR 0257 settles how the two meet. A renewal that arrives on the same session after the conversion grant was sent is the converted lease's first renewal. The primary answers it, although the row is still `local`.

#### 2.5 Unsettled rows after a restore or a forced recovery

A restore or an `--accept-loss` promotion may lack R or the writes after it. So the first write of any restore (plain or forced) and of any `--accept-loss` promotion marks every `expiring` row **unsettled**. This is a new `leases` column, so the mark is in the log for every later primary.

An unsettled row moves only as a `local` lease does (section 2). Only the holder's report clears the mark:
- **`local` at the row's token, or at a higher token while the row names that holder:** the row takes the holder's token and is written `local`.
- **`expiring` at the row's token, or at a higher token while the row names that holder:** the primary settles the lease (section 2) at the holder's token, which writes the row and clears the mark.
- **The row names another holder:** the primary revokes the lease at the token the node reports, which section 5.13 accepts because it is "at least the current one". It then raises the row's token above that.

Cost: after any restore, no `warm` app moves on its gate alone until its holder reports.

#### 3. Fallback when R cannot be replicated (liveness)

R waits only while a machine of the required set is unreachable or not following. That covers a down node, a node declared lost but not yet removed, and a node on a binary that does not follow. In the demo, it is either node.

Meanwhile:
- **The change itself.** The policy change is in the database, and the API reports the end of the renewals as pending.
- **The node.** The holder still holds the lease `expiring`: it renews, and the app keeps running. Its exposure is warm's: it fences only if the primary loses majority support.
- **The operator** waits, or removes the node (section 10). Removal is not refused for leaving fewer than three voters, and the HA view then reports that HA needs a third vote. The removal entry must itself reach the remaining nodes before the removed node leaves the required set (section 1).

No setup is permanently stuck.

#### 4. Restore and forced recovery

`quoro restore` (backup-format.md section 8.2) brings back a log that may lack R. It also reuses that log's (term, seq) pairs.

**When a restore is refused.** From M5b, `quoro restore` refuses when another machine with the `node` role that is not `removed` appears in the restored database, or in the newest `spec.votes.voters` stored by the restored host's agent. The second source covers nodes enrolled after the backup.
- Such a cluster recovers by promoting a surviving node (section 7.3), which keeps every replicated entry.
- A cluster whose only node is the restored host's own restores as today, plus the unsettled mark (section 2.5).

**Override.** `quoro restore --accept-loss --statement <text>` restores anyway.
- **The statement** says that no other machine with the `node` role survives, and that no control plane of this cluster runs. The first part also rules out a concurrent promotion, because only such a machine can be a candidate. The second rules out an old primary on a host without the `node` role that keeps granting.
- **Removing the dead machines.** The restored primary's first write removes every machine with the `node` role that the statement declares gone. It raises the token of each lease they held, as declaring a node lost does (section 8.3), and deletes their seals, in that one write. The write is exempt from section 8.10 rule 2. Without it, the dead nodes would count in every majority. The restored primary would then never hold support, and could neither remove nor enroll a node.
- **A single-node cluster.** Restored onto a fresh host, it needs the flag, because the dead node is not the new host's node.
- **Term.** The restored primary starts one above the highest of:
  - the restored log's terms;
  - the term its host's agent stored;
  - the terms in any vote `check` answers it receives (a `check` changes nothing).

  A fresh host is no mesh peer and may reach no voter. So each `stale_term` error gains the sender's term. A restored `--accept-loss` primary that receives one before it has granted anything starts again one above that term, instead of demoting itself.

**Tokens after a forced recovery.** A token issued in lost entries may be issued again to another node, as section 7.3 already says for `--ack-fence`.

### What it changes in accepted ADRs

- **ADR 0255 (D-0255), refined in two places.**
  - "Committed on a majority of the voters" becomes "acknowledged by every machine of the required set".
  - Every `local` grant of this design needs majority support, which narrows section 8.8's "`local` leases need no support" for these grants only.

  Its sentence "until the lease row is local and replicated, the lease stays expiring and keeps renewing" is kept as written. ADR 0255 is completed with the resend, and ADR 0256 part 2 with its reading.
- **ADR 0169 stands.** Its "no log-commit quorum: both are M6 (D-0016)" still holds. This design is full replication across the node-role machines for one purpose, not a commit quorum, and the vote is unchanged.
- **Section 7.3 step 1 (ADR 0182, ADR 0232, #405).** It stays true for entries that some node never acknowledged. It is no longer true for an entry every node acknowledged, except under `--accept-loss`.
- **Refined:**
  - ADR 0182 and ADR 0232: the candidate builds from the log it froze, takes its term from the vote answers, and gains `--accept-loss`.
  - ADR 0179 and ADR 0227: durable acknowledgements, the mark, the refollowing state, and acknowledgements reset on a divergence.
  - ADR 0148 and backup-format.md section 8.2: the restore refusal, its override, its first write and its term.
- **Unchanged:** ADR 0236 (`--ack-fence` still skips only the voters' wait) and section 8.9's vote rules.
- **Supersedes:** no accepted ADR.

### Alternatives rejected

- **Majority commit with Raft's vote rule.** With three or more nodes, it ends the renewals while one node is down. It needs:
  - log positions in the vote, with a rule that the candidate's log is at least as current as the voter's;
  - Raft's restriction to acknowledgements of the current term;
  - a freeze at vote time on every voter;
  - a rule that a voter-set change keeps every committed entry committed;
  - a vote-based term for a forced restore.

  Each one was needed against a concrete counterexample: an entry of an earlier term counted committed, an acknowledgement sent after a vote, a committed entry lost through voter-set changes in a witness-heavy set, and an equal-term restore. That is Raft's log-commit quorum, which ADR 0169 places in M6 with D-0016.
- **Keeping the row `expiring` until the grant is sent, and deriving "possibly local" from entry order.** A row write made for another reason in between ends that state while the holder may already run `local`. Examples are a maintenance window's raise to `W` and a conversion's first answer.
- **D-0255 as written, with a majority of the voters and witnesses included.** Witnesses never acknowledge, and without a log check in the vote, the winner need not hold R.
- **Changing the mode with a new token.** That means revoke, fence, then `local` at token + 1. It is unsafe without this rule, for the reason under "Problem", and it restarts the app.
- **A time-limited fallback to the new-token path while a node stays down.** Unsafe for the same reason, at the moment it would be used.
- **Never ending an expiring lease's renewals.** A `none` app would then fence whenever the primary loses majority support. That breaks what `none` means during a control-plane outage (D-0011).
- **Candidate catch-up from other nodes.** It needs a log transfer between nodes over the mesh: more machinery for the same guarantee.

### Wire and cost summary

- **Vote:** no change. The CLI reads the `term` that answers already carry.
- **`stale_term` errors:** gain a `term` field.
- **Node:**
  - fsync before `log_ack` and `hello.log_seq`;
  - the mark next to `custody.key`;
  - the refollowing state and the freeze in the local store (section 12).
- **db-schema:** `leases` gains the unsettled mark.
- **CLI:**
  - `quoro cluster promote --accept-loss`;
  - `quoro restore --accept-loss --statement`;
  - the restore refusal;
  - `cluster.promoted.params.accept_loss`, and a record of a forced restore.
- **Primary:**
  - each node's acknowledged seq, which ADR 0227 already keeps;
  - acknowledgements reset on a divergence;
  - `log_diverged` for a `hello` below the last acknowledgement;
  - the regrant path;
  - the settle with its `ping`.
- **API:** the end of the renewals is reported as pending while R waits.
- **Time:** a change off `warm` usually waits milliseconds. While a node is down or not following, it waits until the node returns or is removed. With three or more nodes this is stricter than a majority, because one down node holds the change.
- **Costs:**
  - A node host rolled back as a whole to a disk snapshot, or whose ack record the primary lost in a crash, can still be promoted from a stale log. Promoting from it is a forced recovery by operator rule, so it needs `--accept-loss`.
  - The restore refusal can miss a node that enrolled after the backup if the host's agent never learned of it.
  - After any restore, `warm` apps wait for their holders to report before they can move.

### Review

Four independent review passes:

- **The first two** reviewed a majority-commit draft and its fixes. They led to this design.
- **The third and fourth** attacked this design. Neither found a way for a sealed candidate to lack the write. They tried joins while it was pending and after, re-enrollment, custody key changes, seals written before it, removal, a node's store lost, and witness-heavy sets.
- **What they found** was in the side paths, and the fixes are written above:
  - the third found six safety and three liveness problems;
  - the fourth found five safety and three liveness problems, and several editorial ones.
- **Not yet reviewed when this was decided:** the fourth pass's fixes. They are: writing the row first, the unsettled mark, the required set across removals, the narrowed regrant, the dead-node removal of a forced restore, the reset of the mark and the acknowledgements, the term from vote answers, and the settle by `pong`.

### Outside this design, found while checking it

Recorded separately: the clock of the primary's support window, and longer grants sent before every node holds the raised `duration_ms`.

_Some wording was changed for the public repository._

## Consequences

- Replaces an internal decision (not published).
- Refines [ADR 0255](0255-a-conversion-of-an-expiring-lease-to-local-is-sent-only.md): what replicated means: acknowledged by every machine with the node role instead of committed on a majority of the voters, and every local grant of this design needs majority support. The rest of [ADR 0255](0255-a-conversion-of-an-expiring-lease-to-local-is-sent-only.md) stands.
- Refines [ADR 0256](0256-an-expiring-grant-that-carries-re-never-converts-a-lease.md): the reading of part 2: the primary answers no renewal of the lease once it has sent the local grant on that session. The rest of [ADR 0256](0256-an-expiring-grant-that-carries-re-never-converts-a-lease.md) stands.
- Refines [ADR 0179](0179-change-log-follow-74-every-machine-with-role-node-follows.md): durable acknowledgements, the high-water mark, the refollowing state, and acknowledgements reset on a divergence. The rest of [ADR 0179](0179-change-log-follow-74-every-machine-with-role-node-follows.md) stands.
- Refines [ADR 0227](0227-change-log-follow-sends-one-log-entries-batch-at-a-time-and.md): durable acknowledgements, the high-water mark, the refollowing state, and acknowledgements reset on a divergence. The rest of [ADR 0227](0227-change-log-follow-sends-one-log-entries-batch-at-a-time-and.md) stands.
- Refines [ADR 0182](0182-manual-promotion-19-quoro-cluster-promote-on-the-candidate.md): the candidate builds from the log it froze, takes its term from the vote answers, and gains --accept-loss. The rest of [ADR 0182](0182-manual-promotion-19-quoro-cluster-promote-on-the-candidate.md) stands.
- Refines [ADR 0232](0232-quoro-cluster-promote-refuses-on-a-host-that-already-has-a.md): the candidate builds from the log it froze, takes its term from the vote answers, and gains --accept-loss. The rest of [ADR 0232](0232-quoro-cluster-promote-refuses-on-a-host-that-already-has-a.md) stands.
- Refines [ADR 0148](0148-control-plane-backup-vacuum-into-on-the-primary-every-6-h.md): the restore refusal, its override, its first write and its term. The rest of [ADR 0148](0148-control-plane-backup-vacuum-into-on-the-primary-every-6-h.md) stands.
- [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md) refines while a lease row is marked, every grant of the lease, a new token's included, waits until every node holds the row's newest write; R sets the mark and only the holder's report clears it; the frozen log is checked again; a removal counts only machines not removed; and the side paths listed here are fixed or stated as limits. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0261 in the project decision ledger. Do not edit by hand; changes go through a new decision.
