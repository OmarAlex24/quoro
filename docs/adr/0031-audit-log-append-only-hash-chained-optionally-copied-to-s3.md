# ADR 0031: Audit log: append-only, hash-chained, optionally copied to S3/R2 with object lock; MCP uses the same path

- **Status:** Accepted; refined by [ADR 0306](0306-an-audited-api-call-is-always-audited-bound-what-reaches.md)
- **Date:** 2026-11-02
- **Ledger id:** D-0031
- **Tags:** agents, audit, security
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0306](0306-an-audited-api-call-is-always-audited-bound-what-reaches.md)

## Context

Everything must be auditable and loggable; tampering must be evident.

## Decision

Audit log: append-only, hash-chained, optionally copied to S3/R2 with object lock; MCP uses the same path.

Entry: who, on behalf of whom, triggering event, tool/endpoint, parameters, result, agent's stated justification. Off-site copy out of reach of whoever operates the server. MCP has no separate path: same service layer, same log.

## Consequences

- [ADR 0306](0306-an-audited-api-call-is-always-audited-bound-what-reaches.md) refines what an audit record keeps of its params when they would make its change-log entry one a node could not receive alone and verify: their digest and size instead; and the API's request body limit, whose 413 is audited. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0031 in the project decision ledger. Do not edit by hand; changes go through a new decision.
