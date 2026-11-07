# ADR 0262: fence_ack acknowledges a listed lease with no row at 2^53-1, the canonical-JSON bound, and migration 0001's comment names fencing_tokens

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0262
- **Tags:** m5b, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

An earlier draft proposed i64::MAX as the acknowledged token of a listed lease with no row. That value cannot be stored: fence_ack_pending is part of the node payload, every node update appends a change-log entry, and quoro_proto::jcs (the canonical JSON of the change log, audit log and signatures) refuses integers above 2^53-1 = 9007199254740991. With i64::MAX the POST /api/v1/nodes/{node_id}/fence-ack call fails with 500 internal_error and nothing is recorded (seen in the test a_revoke_answered_fence_failed_lists_its_lease). 2^53-1 is the highest value the change log can carry. It is still above every token a node can hold from a primary: every lease grant and token increment appends the lease row, fencing_token included, to the change log (crates/control-plane/src/leases.rs), so a primary cannot issue a token above 2^53-1, and that change log is what replicas and a restore come from. The rest of that draft is unchanged.

## Decision

fence_ack acknowledges a listed lease with no row at 2^53-1, the canonical-JSON bound, and migration 0001's comment names fencing_tokens.

Only the value and the reason for it changed from the earlier draft. (1) Section 8.5 step 3: for a listed lease with no row, the primary stores 2^53-1 = 9007199254740991 (the canonical-JSON safe-integer bound) as its acknowledged token, so the node clears every failed fence of that lease below it. Why it stays safe, written after bullet 2 of 'Why the token is enough' for a lease another node holds: the primary grants a lease it has no row for to no node, and sends the node nothing about it except the revoke of section 5.1 at each session start, which the node answers before the fence_ack goes out (step 4, ADR 0239); a fence_failed answer takes the lease out of the acknowledgement (ADR 0238). So every failed fence the node records for it before the acknowledgement is delivered is one the operator saw or one that is listed again. Range: a hello's fencing_token is a u64 the primary does not range-check, and the primary revokes a lease it has no row for at the claimed token, so a node could claim a token above 2^53-1. No primary can have granted it (see Why), so it means a node bug or a corrupted or forged node store. The node then records the failed fence at that token, the acknowledgement's 2^53-1 is below it, the node keeps the failure and stays failed, and no fence_ack can clear it. That is the safe side (the node starts nothing stateful for the lease). The same already holds for a lease with a row, whose acknowledged token comes from the row (ADR 0245, ADR 0247). A failed fence at exactly 2^53-1 is not cleared either; it takes 2^53-1 ownership changes of one resource. Rejected as before: store the failure's token on the primary (raised in the review of #445), because NodeFenceFailure in openapi.yaml has no token field. Without it, a listed lease with no row would get no stored token, and an acknowledgement that lists it would go out without fencing_tokens, as a primary before ADR 0245 sends it: the node would clear every failed fence of every listed lease by lease id. (2) Unchanged: a comment-level note for the next migration or db-schema.sql: fence_ack_pending is {acked_at, lease_ids, fencing_tokens: {lse_: integer}}, with fencing_tokens absent on rows a primary before ADR 0245 wrote; such a row is delivered without fencing_tokens. Implementation: #546, with its own test. No wire change.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0262 in the project decision ledger. Do not edit by hand; changes go through a new decision.
