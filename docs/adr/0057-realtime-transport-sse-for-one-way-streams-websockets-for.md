# ADR 0057: Realtime transport: SSE for one-way streams, WebSockets for bidirectional sessions

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0057
- **Tags:** stack, architecture
- **Supersedes:** [ADR 0044](0044-realtime-transport-websockets-vs-sse-stays-open-until-the.md)
- **Superseded by:** none
- **Related:** none

## Context

D-0044 left WebSockets vs SSE open until the architecture doc. The two carry different traffic: most panel streams are server-to-client, a few sessions are interactive. The maintainer accepted it on 2026-11-02.

## Decision

Realtime transport: SSE for one-way streams, WebSockets for bidirectional sessions.

SSE (text/event-stream) for server-to-client streams: the event stream (GET /api/v1/events/stream), live app logs (GET /api/v1/apps/{app_id}/logs/stream), deployment progress. Reason: plain HTTP, passes reverse proxies and Cloudflare without upgrade handling, auto-reconnect with Last-Event-ID maps directly to the event/log seq cursor. WebSockets for bidirectional interactive sessions only: web terminal and VM console (M4, xterm.js). The node-agent dial-out session (node-agent-protocol.md) also uses a WebSocket, but that is a protocol choice outside D-0044's panel scope. axum serves both natively (axum sse module and ws feature; not re-verified in docs/research, hypothesis). Cost: two mechanisms instead of one; benefit: no WebSocket for the common read-only case. Referenced in docs/architecture/architecture.md section Realtime transport.

_Some wording was changed for the public repository._

## Consequences

- Replaces ADR 0044.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0057 in the project decision ledger. Do not edit by hand; changes go through a new decision.
