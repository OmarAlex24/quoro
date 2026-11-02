# ADR 0014: Telemetry (app logs, metrics) stays on each node; only state is replicated

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0014
- **Tags:** architecture, observability
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Shipping all logs to the primary would saturate it and make it the weak point.

## Decision

Telemetry (app logs, metrics) stays on each node; only state is replicated.

Panel queries node telemetry through the primary on demand; optional archive to S3/R2.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0014 in the project decision ledger. Do not edit by hand; changes go through a new decision.
