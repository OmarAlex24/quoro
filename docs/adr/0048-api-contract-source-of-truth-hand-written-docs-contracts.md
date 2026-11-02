# ADR 0048: API contract source of truth: hand-written docs/contracts/openapi.yaml is frozen; utoipa output is diffed against it in CI; orval generates the panel client

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0048
- **Tags:** stack, api
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0028 says OpenAPI via utoipa (code-first); D-0033/D-0036 make the OpenAPI spec an M0 contract that freezes before code exists. Both hold only if the hand-written spec is the reference and the generated one must match it.

## Decision

API contract source of truth: hand-written docs/contracts/openapi.yaml is frozen; utoipa output is diffed against it in CI; orval generates the panel client.

M0 freezes docs/contracts/openapi.yaml. The control plane annotates handlers with utoipa; CI generates the document and fails on any semantic diff against the frozen file. Every response carries a non-empty description (works around utoipa #1636 / orval rejection, F-0042). TS client: orval (MIT, 8.x) over @hey-api/openapi-ts (pre-1.0), both handle 3.1 (docs/research/rust-api-and-mcp-tooling.md). Pin rmcp to a minor version and budget major upgrades (F-0041). This refines D-0028's 'orval or openapi-ts'; the noVNC removal is decided separately in D-0056.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0048 in the project decision ledger. Do not edit by hand; changes go through a new decision.
