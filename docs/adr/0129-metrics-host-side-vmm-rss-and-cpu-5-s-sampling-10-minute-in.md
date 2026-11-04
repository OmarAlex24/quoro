# ADR 0129: Metrics: host-side VMM RSS and CPU, 5 s sampling, 10-minute in-memory ring, SSE samples carry no id

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0129
- **Tags:** contracts, m4
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

No guest agent; nothing stored or replicated (ADR 0014)

## Decision

Metrics: host-side VMM RSS and CPU, 5 s sampling, 10-minute in-memory ring, SSE samples carry no id.

Reconnecting clients re-read history from the GET endpoint.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0129 in the project decision ledger. Do not edit by hand; changes go through a new decision.
