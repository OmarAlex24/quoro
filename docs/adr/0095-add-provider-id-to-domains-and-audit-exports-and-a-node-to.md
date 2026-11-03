# ADR 0095: Add provider_id to domains and audit_exports and a node-to-tunnel-provider mapping so DELETE /providers/{id} can return 409 while referenced

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0095
- **Tags:** providers, contract
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

openapi already promises the 409; the schema must make the reference checkable (Q-0091 option a).

## Decision

Add provider_id to domains and audit_exports and a node-to-tunnel-provider mapping so DELETE /providers/{id} can return 409 while referenced.

Same standalone contract PR as the tunnel-token decision. The providers delete handler then replaces its TODO with the reference check.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0095 in the project decision ledger. Do not edit by hand; changes go through a new decision.
