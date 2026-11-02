# ADR 0028: Baseline stack (to be confirmed in the architecture doc)

- **Status:** Superseded by [ADR 0056](0056-baseline-stack-without-novnc-consoles-are-text-only-xterm.md)
- **Date:** 2026-11-02
- **Ledger id:** D-0028
- **Tags:** stack
- **Supersedes:** none
- **Superseded by:** [ADR 0056](0056-baseline-stack-without-novnc-consoles-are-text-only-xterm.md)
- **Related:** none

## Context

Rust for everything touching infra; React panel embedded in the binary.

## Decision

Baseline stack (to be confirmed in the architecture doc).

Control plane/API: Rust Axum + Tokio, sqlx + SQLite. Node agent: Rust static binary. MCP: rmcp (official Rust SDK). API contract: OpenAPI via utoipa -> TS client (orval or openapi-ts). Panel: React + TanStack Router/Query/Table, shadcn/ui + Tailwind, embedded with rust-embed. Consoles/metrics: xterm.js, noVNC, uPlot. Realtime: WebSockets or SSE. bollard (Docker) no longer needed since there is no Docker on nodes. The maintainer prefers minimal dependencies; every dependency must be justified.

_Some wording was changed for the public repository._

## Consequences

- Superseded by [ADR 0056](0056-baseline-stack-without-novnc-consoles-are-text-only-xterm.md); this decision is no longer binding and is kept for history.

---
Generated from decision D-0028 in the project decision ledger. Do not edit by hand; changes go through a new decision.
