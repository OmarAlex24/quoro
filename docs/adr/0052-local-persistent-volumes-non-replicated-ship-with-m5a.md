# ADR 0052: Local persistent volumes (non-replicated) ship with M5a, before the cold-backup DoD; until then the PRD states no persistent app data

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0052
- **Tags:** engine, sync, roadmap
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Volumes were shape-only until M5, desired_state.volumes is [] before M5, and no endpoint creates or attaches a volume, yet the M5a DoD backs up a stateful app with data; from M2 to M4 a database app loses its data on every redeploy.

## Decision

Local persistent volumes (non-replicated) ship with M5a, before the cold-backup DoD; until then the PRD states no persistent app data.

Options: (a) M5a: local non-replicated volumes (create, attach to an app, survive redeploy) arrive with cold backups; standalone post-freeze contract PR before M5a; PRD non-goals say 'no persistent app data before M5a'. (b) M4: volumes ship with the full experience, so the first public release (Q-0006) can run a database; adds scope to M4. (c) M2: volumes with the first deploy; adds scope to the riskiest milestone. Accepted option: (a), the minimal reading of D-0033.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0052 in the project decision ledger. Do not edit by hand; changes go through a new decision.
