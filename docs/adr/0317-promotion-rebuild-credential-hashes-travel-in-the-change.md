# ADR 0317: Promotion rebuild: credential hashes travel in the change log, entries fold per entity, and a row with no create stops the rebuild

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0317
- **Tags:** m5b, promotion, contracts, changelog
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0179](0179-change-log-follow-74-every-machine-with-role-node-follows.md), [ADR 0226](0226-node-credential-hashes-travel-in-the-change-log-from-schema.md)

## Context

The rebuild of node-agent protocol section 7.3 step 4 cannot recreate several rows from the log as written. Three points needed the maintainer: whether token and key hashes travel in the log, whether a per-entity fold counts as applying entries in seq order, and what an update without a row does.

## Decision

Promotion rebuild: credential hashes travel in the change log, entries fold per entity, and a row with no create stops the rebuild.

1. Credential hashes. api_tokens.token_hash, agent_keys.key_hash and node_join_tokens.token_hash travel in the change log, on the reasoning of ADR 0226: a SHA-256 of a random secret gives a follower no usable credential. API tokens and agent keys then survive a promotion, which a recovery run by agents needs. Section 7.1 names them.

2. The fold. The rebuild applies entries in seq order to an in-memory row per entity: create replaces, update merges, delete removes. It then inserts the final rows in one transaction with foreign keys deferred and applies the ON DELETE CASCADE deletes itself. Each entry is still copied into change_log in order and both chains are verified. Section 7.3 step 4 gets a line that says this; the fold is not left to a reading of 'in seq order'.

3. An update without a row. During the fold, an update for an entity that has no row merges into an empty row, so a log that holds an update before its create still rebuilds. At the end of the fold, an entity that never received a create stops the rebuild with an error naming the entity and the seq of its first entry. The rebuild never inserts a row made only of updates.

The maintainer approved the three points on 2026-11-10. Points 2 and 3 are the cautious readings; they stay open to a superseding decision until the rebuild (part E of the promotion work) merges. Contract changes go in a standalone contract change.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0179](0179-change-log-follow-74-every-machine-with-role-node-follows.md): how a promotion rebuilds the database from the log: a fold per entity, and an entity with no create stops it. The rest of [ADR 0179](0179-change-log-follow-74-every-machine-with-role-node-follows.md) stands.
- Refines [ADR 0226](0226-node-credential-hashes-travel-in-the-change-log-from-schema.md): which credential hashes travel in the change log: also those of API tokens, agent keys and join tokens. The rest of [ADR 0226](0226-node-credential-hashes-travel-in-the-change-log-from-schema.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0317 in the project decision ledger. Do not edit by hand; changes go through a new decision.
