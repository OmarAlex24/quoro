# ADR 0123: M4 build scope: public HTTPS repos only, trigger api only, one active build per app, fixed default limits

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0123
- **Tags:** contracts, m4
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Smallest surface for the DoD; private repos and push triggers need credential and webhook design

## Decision

M4 build scope: public HTTPS repos only, trigger api only, one active build per app, fixed default limits.

Limits {vcpus 2, memory_mb 2048, timeout_s 1800, cache_gb 10, output_gb 10}; a second build while one is active answers 409; startBuild and cancelBuild answer 202; build log on node capped 8 MiB, last 20 builds per app; getBuildLogs max 1000 lines per page.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0123 in the project decision ledger. Do not edit by hand; changes go through a new decision.
