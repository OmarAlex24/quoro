# ADR 0292: failoverApp answers 202 at once while the grant of a marked lease waits, and the lease shows the wait (grant_pending, grant_waiting_on)

- **Status:** Accepted; refined by [ADR 0295](0295-a-restarted-primary-sends-a-marked-row-s-pending-new-token.md)
- **Date:** 2026-11-09
- **Ledger id:** D-0292
- **Tags:** m5b, leases, contracts, warm, failover
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0285](0285-a-volume-restore-that-turns-an-expiring-lease-row-local-at.md), [ADR 0295](0295-a-restarted-primary-sends-a-marked-row-s-pending-new-token.md)

## Context

D-0285 keeps failoverApp's request open until the grant of a marked row is sent, which for a declared-lost holder lasts until an operator removes that node. A client, a retry with the same Idempotency-Key and an agent's MCP call then time out with no signal of what the failover waits for. Decided before the primary's failover is implemented.

## Decision

failoverApp answers 202 at once while the grant of a marked lease waits, and the lease shows the wait (grant_pending, grant_waiting_on).

### Problem

D-0285 says that `failoverApp` "answers once the grant is sent". The waits can be long:

- The grant of a marked row waits until every machine with the node role holds the row's newest write and the primary holds majority support.
- A declared-lost holder stays in that set until it is removed. So when the holder is declared lost, the grant waits for an operator to run removeNode, or for that node to come back.

Holding the request open for that long has costs:

- An HTTP client times out.
- A retry with the same Idempotency-Key finds the first request still running.
- An agent's MCP call times out with no answer.
- The panel cannot show what the failover waits for.

Today the caller gets no signal of the wait other than the request hanging.

### Proposal

`failoverApp` answers 202 at once, and the wait shows in the lease.

1. **The answer.** `failoverApp` answers 202 with the app once the change that moves the lease commits. At that point:
   - the row names the warm node at the new token;
   - `unsettled` is true;
   - the standby VM is `running` in the warm node's desired state.

   The grant to the warm node follows under "Grants of a marked row". This needs no new safety rule: the warm node starts the VM only while it holds the lease `held` with the spec's token (node-agent-protocol.md section 6.4), and the row-first, grant-after order is already the marked-row rule.
2. **While the grant waits,** a second `failoverApp` or a `switchoverApp` of the app answers `409 conflict` with `reason: in_progress`, as for a running failover.
3. **Two new required fields on Lease,** both derived on read and never stored:
   - `grant_pending` (boolean) is true while the primary holds a grant of this lease that it has not sent, because it waits for the row's newest write to be replicated and for majority support.
   - `grant_waiting_on` (array of node ids) lists the machines with the node role that have not acknowledged the row's newest write while `grant_pending` is true. It is empty otherwise. It is empty too when only majority support is missing.
4. **`app.failed_over`** is emitted, as today, in the change that moves the lease. Its data gains an optional `grant_pending` boolean, so a webhook consumer knows that the start waits.

### What a client polls

The client calls `getApp` (`GET /api/v1/apps/{app_id}`) after the 202. The response moves through three states:

1. **Waiting.** `App.lease.holder_node_id` is the warm node, and `grant_pending` is true. `grant_waiting_on` names the nodes the grant waits for. A declared-lost holder listed there is the operator's cue to remove it with `removeNode`. The panel and the CLI show this.
2. **Grant sent.** `grant_pending` is false and `unsettled` is still true: the warm node has the grant and is starting the app.
3. **Done.** `unsettled` is false: the holder renewed and the primary settled the row. The app's instance on the warm node is `running`.

Neither the CLI nor MCP has a failover command yet. When they gain one:

- the CLI prints the wait, and with `--wait` polls `getApp` every 2 s until `unsettled` is false, printing `grant_waiting_on` while `grant_pending` is true;
- the MCP tool returns the app at once, and an agent polls `get_app`.

A client can also follow `/api/v1/events/stream` for `app.failed_over`.

### Exact contract changes

#### openapi.yaml, `failoverApp` description

- **The `lease_held` bullet.** Replace "The call is not refused for that wait: it answers once the grant is sent (ADR 0285), so the request stays open until then." with: "The call is neither refused for that wait nor held open by it: it answers once the change that moves the lease commits, and the grant follows (this decision). `App.lease.grant_pending` and `App.lease.grant_waiting_on` show the wait."
- **The main paragraph.** Replace "While the row carries the unsettled mark, the lease has moved once its grant to the warm node is sent (above)." with: "While the row carries the unsettled mark, the grant to the warm node is sent once the row's newest write is replicated over every machine with the `node` role and the primary holds majority support (node-agent-protocol.md section 8.4, 'Grants of a marked row'). Until then `App.lease.grant_pending` is true, and the standby VM stays stopped, because the warm node does not hold the lease (section 6.4)."
- **The `in_progress` bullet.** It becomes: "a failover or switchover of the app is running, or its grant to the warm node waits (`App.lease.grant_pending`)".
- **The 202 description.** It becomes: "The lease moved to the warm node in the primary's state. The app starts there once the warm node holds the lease (`App.lease.grant_pending`)."

#### openapi.yaml, Lease schema

Add `grant_pending` and `grant_waiting_on` to `required`, and add these properties:

```yaml
grant_pending:
  type: boolean
  description: |
    True while the primary holds a grant of this lease that it has not sent:
    the row carries the unsettled mark, and its newest write is not yet
    replicated over every machine with the `node` role, or the primary does
    not hold majority support (node-agent-protocol.md section 8.4, "Grants of
    a marked row"). Derived on read; never stored. False otherwise.
grant_waiting_on:
  type: array
  items:
    type: string
    pattern: '^nod_[0-9A-HJKMNP-TV-Z]{26}$'
  description: |
    While `grant_pending` is true, the machines with the `node` role that have
    not acknowledged the row's newest write, a declared-lost node included
    until it is removed. Empty otherwise, and empty when only majority support
    is missing.
```

`crates/control-plane/src/api/leases.rs` serves both fields. Until the primary implements the marked-row grant, they are `false` and `[]`, as `unsettled` is today.

#### node-agent-protocol.md section 8.4, "Grants of a marked row"

Replace "`failoverApp` is not refused for it: it answers once the grant is sent, so the request stays open until then (ADR 0285)." with: "`failoverApp` is neither refused for it nor held open by it: it answers `202` once the change that moves the lease commits, and the grant follows (this decision). Until the grant is sent, the primary reports the wait in `Lease.grant_pending` and `Lease.grant_waiting_on`, and a second failover or a switchover of the app answers `in_progress`."

#### events.schema.json, `data_app_failed_over`

- **The description.** Replace "Emitted by the control plane when it grants the lease" with: "Emitted by the control plane in the change that moves the lease; while the lease row carries the unsettled mark, the grant to the warm node follows once that change is replicated (node-agent-protocol.md section 8.4)".
- **A new optional property:** `"grant_pending": { "type": "boolean", "description": "True when the grant to the warm node waits for replication and majority support, so the app starts later." }`.
- `additionalProperties` is already true, so the change is additive.

#### threat-model.md

T-LEASE-16 and T-LEASE-01 need no change. The safety argument does not move, because the warm node still starts only on a held grant.

### Cost

- Two derived fields on Lease and one optional field on the event.
- The primary must compute `grant_waiting_on` from the acknowledgements it already tracks for the required set.
- No new rule on the node, and no migration.

### Alternatives

- **Keep the synchronous answer (D-0285 as written).** Simple, but the request can hang until an operator acts, and a timed-out retry cannot tell a waiting failover from a lost one.
- **Answer 202 with no new fields.** The client polls `unsettled` only. It cannot tell "the grant waits for node X" from "the warm node is starting". So nothing tells the operator to remove the lost holder.
- **Refuse with `409 conflict` (`grant_waits`) while the grant would wait.** This brings back the refusal that D-0285 chose against, and the operator must call again after removeNode.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0285](0285-a-volume-restore-that-turns-an-expiring-lease-row-local-at.md): how failoverApp answers while a marked lease's grant waits: 202 at once, with the wait shown in Lease.grant_pending and Lease.grant_waiting_on. The rest of [ADR 0285](0285-a-volume-restore-that-turns-an-expiring-lease-row-local-at.md) stands.
- [ADR 0295](0295-a-restarted-primary-sends-a-marked-row-s-pending-new-token.md) refines what a primary restart does to a failover's waiting grant: the grant is sent at the warm node's next session start, and grant_pending is true again until then. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0292 in the project decision ledger. Do not edit by hand; changes go through a new decision.
