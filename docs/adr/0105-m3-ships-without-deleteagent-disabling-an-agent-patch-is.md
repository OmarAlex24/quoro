# ADR 0105: M3 ships without deleteAgent; disabling an agent (PATCH) is the lockout; delete semantics are decided later

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0105
- **Tags:** agents, m3
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-03. Deleting is not implemented and the schema cascade would erase grant and approval history (ADR 0031); disabling already blocks the agent.

## Decision

M3 ships without deleteAgent; disabling an agent (PATCH) is the lockout; delete semantics are decided later.

The question of delete semantics stays open (recommendation on record: soft delete with agents.deleted_at). deleteAgent keeps answering as not implemented until then.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0105 in the project decision ledger. Do not edit by hand; changes go through a new decision.
