# ADR 0233: Signing-key rotation: human-only scope cluster:manage, one rotation at a time, the next key sealed before it is sent, the old one kept until every machine pins the next

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0233
- **Tags:** m5b, contracts, promotion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

#76 asks for a rotation API; the scope, how the primary signs while machines pin different keys, and when the old key may go are open.

## Decision

Signing-key rotation: human-only scope cluster:manage, one rotation at a time, the next key sealed before it is sent, the old one kept until every machine pins the next.

POST /api/v1/cluster/signing-key/rotate under a new owner-only, human-only scope cluster:manage; 409 conflict reason in_progress while a rotation runs. The primary generates the next key, seals it to every machine with a custody key and records it in a replicated signing_keys table (current, next), then sends signing_key_rotate signed by the current key to every machine of any role, connected ones at once and the rest on their next session before desired_state. It tracks the key each machine pins in nodes.signing_key_id (from hello.signing_key_id and the rotate answer) and signs each machine's messages with that key. Once every machine that is not removed pins the next key, one write deletes the old key's row and seals and makes the next current, with the audit record cluster.signing_key_rotated; then the old key file is deleted. Cluster.signing_key_id and Cluster.signing_key_rotation show it. Rejected: switching keys at once, which strands every disconnected machine on a key the primary no longer signs with.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0233 in the project decision ledger. Do not edit by hand; changes go through a new decision.
