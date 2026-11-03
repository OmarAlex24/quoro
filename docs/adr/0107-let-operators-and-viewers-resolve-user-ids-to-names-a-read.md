# ADR 0107: Let operators and viewers resolve user ids to names (a read path for users)

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0107
- **Tags:** contracts, panel, m3
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

GET /api/v1/users needs user:write (owner only), so an operator or viewer cannot turn owner_user_id, on_behalf_of, decided_by or created_by into a name; the M3 audit DoD asks the entry to show the owner and the on-behalf-of human.

## Decision

Let operators and viewers resolve user ids to names (a read path for users).

Options: (a) a user:read scope in the role bundles (operator and viewer get it) on GET /api/v1/users; (b) embed {id,name} summaries for referenced users in AuditRecord/Agent/Approval responses. Accepted option: (a), the smaller change. Until then the panel shows the name for the owner (who can list users) and for the signed-in user ('you'), and the id otherwise.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0107 in the project decision ledger. Do not edit by hand; changes go through a new decision.
