# ADR 0056: Baseline stack without noVNC: consoles are text-only (xterm.js) because Cloud Hypervisor has no VNC or display device

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0056
- **Tags:** stack, engine, console
- **Supersedes:** [ADR 0028](0028-baseline-stack-to-be-confirmed-in-the-architecture-doc.md)
- **Superseded by:** none
- **Related:** none

## Context

Q-0010 research (docs/research/cloud-hypervisor.md): Cloud Hypervisor v54 has no VNC, SPICE, virtual GPU or framebuffer device; consoles are serial/virtio-console only. noVNC in D-0028 would have nothing to attach to. The maintainer accepted it on 2026-11-02.

## Decision

Baseline stack without noVNC: consoles are text-only (xterm.js) because Cloud Hypervisor has no VNC or display device.

Same as D-0028 except the consoles line. Control plane/API: Rust Axum + Tokio, sqlx + SQLite. Node agent: Rust static binary. MCP: rmcp (official Rust SDK). API contract: OpenAPI via utoipa -> TS client (orval or openapi-ts). Panel: React + TanStack Router/Query/Table, shadcn/ui + Tailwind, embedded with rust-embed. Consoles: xterm.js attached to the microVM serial or virtio-console UNIX socket, relayed by the node agent (or a vsock in-guest agent). No noVNC: Cloud Hypervisor has no graphical console; a graphical console would need a guest-side VNC server or GPU passthrough and is out of scope until an Engine with a display device exists. Metrics: uPlot. Realtime: decided separately in D-0057. bollard not needed. Every dependency must be justified.

_Some wording was changed for the public repository._ _Some operational details were redacted for the public repository._

## Consequences

- Replaces ADR 0028.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0056 in the project decision ledger. Do not edit by hand; changes go through a new decision.
