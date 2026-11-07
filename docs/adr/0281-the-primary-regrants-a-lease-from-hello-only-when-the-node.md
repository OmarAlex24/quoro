# ADR 0281: The primary regrants a lease from hello only when the node lists it expired or refused a same-token local grant, never when it lists it revoked

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0281
- **Tags:** m5b, leases, contracts
- **Supersedes:** [ADR 0264](0264-after-a-node-fences-a-converted-lease-that-restarted-before.md)
- **Superseded by:** none
- **Related:** none

## Context

The maintainer, 2026-11-07, on a blocker that an independent review of the contract for D-0261 found. D-0264 also regranted a lease the node listed as revoked. A successor that lacks a restore's entries cannot tell a revoke for a move from a crash between a revoke and its grant, so it would hand the lease back to the old node while another node runs the app. Both cases behind D-0264 (an agent restart before a converted lease's first renewal, and a deadline that passed while the session was down) come back expired, so limiting the regrant costs no liveness.

## Decision

The primary regrants a lease from hello only when the node lists it expired or refused a same-token local grant, never when it lists it revoked.

Replaces D-0264; its other conditions stand. When a node's hello lists a lease as expired, or the node refused a same-token local grant for it, and the primary's row gives that lease to that node (holder_node_id is the node and revoked_at is null), and the lease is in neither fence_failure nor fence_ack_pending, the primary grants the same node a strictly higher token with the mode the app's policy wants. It writes the row first, and sends the grant only under the replication rule of D-0261 and D-0280 when that rule applies. The primary never regrants a lease the node lists as revoked at or above the row's token. A lease listed below the row's token is revoked as section 5.1 says.

_Some wording was changed for the public repository._

## Consequences

- Replaces ADR 0264.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0281 in the project decision ledger. Do not edit by hand; changes go through a new decision.
