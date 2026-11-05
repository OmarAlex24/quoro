# ADR 0155: Defer the opt-in automatic 'rebuild on critical finding' policy (D-0138 layer 3) to M5b; M5a ships findings, the app.vulnerable incident and the manual 'rebuild with refreshed base'

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0155
- **Tags:** m5a, roadmap, scope, security
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

M5a is the largest milestone so far; automatic rebuilds need deploy health checks and rollback rules tested on top of the new findings pipeline.

## Decision

Defer the opt-in automatic 'rebuild on critical finding' policy (D-0138 layer 3) to M5b; M5a ships findings, the app.vulnerable incident and the manual 'rebuild with refreshed base'.

An agent woken by app.vulnerable can still trigger the rebuild under its grant (D-0030, D-0032), which covers the AI-native story in M5a. Recommendation: accept.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0155 in the project decision ledger. Do not edit by hand; changes go through a new decision.
