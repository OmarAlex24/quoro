# ADR 0030: Grants: scope + resource selector + TTL (+ optional max uses); incident-scoped; JIT approval for destructive actions

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0030
- **Tags:** agents, security, auth
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Agents must have very well defined scopes and not forever.

## Decision

Grants: scope + resource selector + TTL (+ optional max uses); incident-scoped; JIT approval for destructive actions.

Example: app:restart on project=blog for 1h. When an event wakes an agent it gets an incident grant with just the needed scopes, auto-expiring. Destructive/irreversible actions are off by default: the agent files an approval request, a human approves in the panel, the agent gets a short-lived extra grant. Tokens are short-lived and refreshed against the grant; revoking the grant locks the agent out within seconds. Scope examples: read, restart, redeploy, delete.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0030 in the project decision ledger. Do not edit by hand; changes go through a new decision.
