# ADR 0120: M4 builds and metrics reuse existing scopes; M4 adds no MCP tool

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0120
- **Tags:** contracts, m4
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Conservative: no new grantable surface beyond app:terminal and vm:console

## Decision

M4 builds and metrics reuse existing scopes; M4 adds no MCP tool.

startBuild/cancelBuild need app:deploy, build records app:read, build logs app:logs; node metrics node:read, app metrics app:read. MCP tools stay unchanged in M4 (README states it).

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0120 in the project decision ledger. Do not edit by hand; changes go through a new decision.
