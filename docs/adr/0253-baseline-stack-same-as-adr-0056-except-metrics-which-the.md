# ADR 0253: Baseline stack: same as ADR 0056 except metrics, which the panel draws with its own canvas charts instead of uPlot

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0253
- **Tags:** stack, panel
- **Supersedes:** [ADR 0056](0056-baseline-stack-without-novnc-consoles-are-text-only-xterm.md)
- **Superseded by:** none
- **Related:** none

## Context

Decided on 2026-11-06 with the panel redesign: the panel draws its own canvas charts, which updates the metrics line of ADR 0056. The panel never adopted uPlot: today's metric-chart.tsx is hand-written SVG. The redesign's charts fill the area with two-tone ordered dither, which a canvas of about 150 lines draws directly and uPlot would only approximate as a fill pattern.

## Decision

Baseline stack: same as ADR 0056 except metrics, which the panel draws with its own canvas charts instead of uPlot.

Same as ADR 0056 except the metrics line. Control plane/API: Rust Axum + Tokio, sqlx + SQLite. Node agent: Rust static binary. MCP: rmcp (official Rust SDK). API contract: OpenAPI via utoipa -> TS client (orval or openapi-ts). Panel: React + TanStack Router/Query/Table, shadcn/ui + Tailwind, embedded with rust-embed. Consoles: xterm.js attached to the microVM serial or virtio-console UNIX socket, relayed by the node agent (or a vsock in-guest agent). No noVNC: Cloud Hypervisor has no graphical console; a graphical console would need a guest-side VNC server or GPU passthrough and is out of scope until an Engine with a display device exists. Metrics: the panel's own canvas charts (dithered area, crosshair with the exact value, keyboard and screen-reader text alternative), no charting dependency; the data stays the ADR 0129 ring. Realtime: decided separately in ADR 0057. bollard not needed. Every dependency must be justified.

_Some wording was changed for the public repository._

## Consequences

- Replaces ADR 0056.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0253 in the project decision ledger. Do not edit by hand; changes go through a new decision.
