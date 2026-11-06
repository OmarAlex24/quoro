# ADR 0183: Add POST /api/v1/nodes/{node_id}/declare-lost (human-only, audited): it bumps the token of every lease the node holds so their apps can start elsewhere, as node-agent-protocol 8.3 already describes

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0183
- **Tags:** m5b, control-plane, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Section 8.3 says declaring a node lost 'needs an endpoint that a later contract pull request adds'; it was never added. Today a cold restore from a disconnected node needs removeNode, which refuses a node with running instances, and a disconnected node's instances still read running (#342).

## Decision

Add POST /api/v1/nodes/{node_id}/declare-lost (human-only, audited): it bumps the token of every lease the node holds so their apps can start elsewhere, as node-agent-protocol 8.3 already describes.

Scope node:fence_ack's sibling node:declare_lost, human-only. In one transaction: increment the fencing token of each lease held by the node, revoke them in the primary's view, mark the node lost (it stays enrolled), audit record declareNodeLost. On the node's next hello the stale leases are revoked (section 9). Used by cold restores of a disconnected node and by promotion's --ack-fence for the old primary's own node. The maintainer accepted this on 2026-11-06; it is a small change.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0183 in the project decision ledger. Do not edit by hand; changes go through a new decision.
