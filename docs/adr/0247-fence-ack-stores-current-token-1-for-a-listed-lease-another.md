# ADR 0247: fence_ack stores current token + 1 for a listed lease another node holds, so the node can clear a failure recorded at that token

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0247
- **Tags:** m5b, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0245](0245-fence-ack-carries-each-lease-s-acknowledged-token-and-the.md)

## Context

ADR 0245 and ADR 0246 can leave a node failed with no way to clear it. Node A holds L at token 7, is cut off, its fence at valid_until fails (recorded at 7). After the gate the primary fails L over to B at token 8. A reconnects; its hello names L at 7, and the primary revokes L at max(row token, claimed) = 8 (crates/control-plane/src/session/mod.rs revoke_stale_leases on main, matching section 5.1 and the 'Why the token is enough' bullet 3). The revoke's fence fails again on A and, per ADR 0246, is recorded at 8. The operator acknowledges L: step 3 leaves B's token at 8 (ADR 0246), fence_ack_pending stores 8, and the node clears only failed fences below 8 (ADR 0245). The failure at 8 stays, the node stays failed, the primary lists L again, and every new acknowledgement stores 8 again while B holds L. Safe side (A starts nothing stateful) but unrecoverable short of removing the node, and L stays ungrantable while listed.

## Decision

fence_ack stores current token + 1 for a listed lease another node holds, so the node can clear a failure recorded at that token.

Section 8.5 step 3: for a listed lease that another node holds, the primary leaves the lease row as it is (ADR 0246) and stores in fence_ack_pending the row's token plus one, instead of the row's token. For a lease the node still holds nothing changes (the bumped token is stored). The node rule stays 'clear failed fences below the acknowledged token' (section 5.17, ADR 0245), so no wire change. Why it stays safe: a failure on A at B's token can only come from a primary revoke at that token, because A never held L at it. A revoke sent before the acknowledgement is answered before fence_ack goes out (ADR 0239), and a fence_failed answer takes L out of fence_ack_pending (ADR 0238). At a later session start the hello stale-lease revoke is answered first, as today. On a live session after step 3 the primary sends A nothing about a lease B holds. Rejected alternative: clear 'at or below' on the node for such leases; the node cannot tell who holds L, and 'at or below' would also clear a failure at the bumped token of a lease it still holds. Text: one sentence in step 3, one in section 5.17 params ('the lease's fencing_token once the acknowledgement was recorded, or that token plus one for a lease another node holds'), and #423 and #430 follow in code.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0245](0245-fence-ack-carries-each-lease-s-acknowledged-token-and-the.md): the token the acknowledgement stores for a listed lease another node holds. The rest of [ADR 0245](0245-fence-ack-carries-each-lease-s-acknowledged-token-and-the.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0247 in the project decision ledger. Do not edit by hand; changes go through a new decision.
