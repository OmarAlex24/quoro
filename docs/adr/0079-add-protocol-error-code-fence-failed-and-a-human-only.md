# ADR 0079: Add protocol error code fence_failed and a human-only operator acknowledgement that clears a node's failed mark

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0079
- **Tags:** reliability, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-02. M0 reuses bad_message for a failed fence and has no way to clear the failed mark (D-0074).

## Decision

Add protocol error code fence_failed and a human-only operator acknowledgement that clears a node's failed mark.

Standalone contract PR in M1: new error code fence_failed in node-agent-protocol; endpoint POST /api/v1/nodes/{node_id}/fence-ack with a human-only scope, audited; until acknowledged, the primary re-grants nothing that node held.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0079 in the project decision ledger. Do not edit by hand; changes go through a new decision.
