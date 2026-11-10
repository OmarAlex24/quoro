# ADR 0226: Node credential hashes travel in the change log from schema 13, so a promoted primary accepts every node's session

- **Status:** Accepted; refined by [ADR 0317](0317-promotion-rebuild-credential-hashes-travel-in-the-change.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0226
- **Tags:** m5b, contracts, promotion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0317](0317-promotion-rebuild-credential-hashes-travel-in-the-change.md)

## Context

Migration 0002 kept node_credentials local and left whether a promoted primary receives the rows to the promotion work. ADR 0179's list of local tables that start empty after a promotion does not name it; if it started empty, no node could open a session with the new primary.

## Decision

Node credential hashes travel in the change log from schema 13, so a promoted primary accepts every node's session.

node_credentials becomes a replicated table: every insert and update is a change-log entry (entity_type node_credential) carrying the whole row. It holds only SHA-256 hashes of 32 random bytes, like api_tokens and agent_keys, so a follower gains no usable credential. A migration cannot write change-log entries, so at its first start on schema 13 the primary writes one entry per existing row before it serves. Rejected: re-enrolling every node after a promotion, which defeats an unattended recovery; sealing the hashes like the keys, which adds a mechanism for data that is not secret.

## Consequences

- [ADR 0317](0317-promotion-rebuild-credential-hashes-travel-in-the-change.md) refines which credential hashes travel in the change log: also those of API tokens, agent keys and join tokens. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0226 in the project decision ledger. Do not edit by hand; changes go through a new decision.
