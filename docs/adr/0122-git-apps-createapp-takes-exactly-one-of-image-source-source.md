# ADR 0122: Git apps: createApp takes exactly one of image/source; source cannot be added or removed in M4; createDeployment 409 for Git apps

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0122
- **Tags:** contracts, m4
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Avoids mixed-mode apps until there is a use case

## Decision

Git apps: createApp takes exactly one of image/source; source cannot be added or removed in M4; createDeployment 409 for Git apps.

Until the builds epic lands, createApp refuses a source with 422 and updateApp with source answers 409 (no Git app exists yet).

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0122 in the project decision ledger. Do not edit by hand; changes go through a new decision.
