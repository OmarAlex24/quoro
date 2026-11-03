# ADR 0106: Add GET /api/v1/agents/{agent_id}/keys to openapi.yaml so the panel can list an agent's keys

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0106
- **Tags:** contracts, panel, agents, m3
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The frozen spec has createAgentKey and revokeAgentKey but no list; the panel agent screen cannot show which keys exist, when they were last used or which to revoke, unless the operator pastes a key id (found while building #104).

## Decision

Add GET /api/v1/agents/{agent_id}/keys to openapi.yaml so the panel can list an agent's keys.

GET /api/v1/agents/{agent_id}/keys, tag agents, x-quoro-scope agent:read, Limit+Cursor, 200 AgentKeyList {items: AgentKey[], next_cursor} (AgentKey already exists, never carries the secret). Until it lands the panel shows the key created in the current dialog (from the create response only) and a revoke-by-key-id form. Standalone contract PR per D-0036/D-0048; control-plane epic implements.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0106 in the project decision ledger. Do not edit by hand; changes go through a new decision.
