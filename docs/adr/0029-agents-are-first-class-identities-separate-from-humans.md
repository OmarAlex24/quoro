# ADR 0029: Agents are first-class identities, separate from humans

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0029
- **Tags:** agents, security
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Know which agent did what, when, on whose behalf and why.

## Decision

Agents are first-class identities, separate from humans.

Agent entity: name, model, owner. Every action records who (agent), on behalf of whom (human who authorized), why (event/incident that woke it).

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0029 in the project decision ledger. Do not edit by hand; changes go through a new decision.
