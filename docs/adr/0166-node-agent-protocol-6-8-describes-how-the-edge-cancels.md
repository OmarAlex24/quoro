# ADR 0166: node-agent-protocol 6.8 describes how the edge cancels requests to a backend that leaves rotation (GET/HEAD retried once, others 502, streaming bodies cut); OPTIONS stays out of the retry set

- **Status:** Accepted
- **Date:** 2026-11-05
- **Ledger id:** D-0166
- **Tags:** m5a, contracts, edge, ingress
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

PR #334 (issue #332) implements the maintainer's decision: requests in flight to a backend that leaves rotation are cancelled. 6.8 today only says 'a request that fails to connect is retried once ... only for GET and HEAD'. The behaviour stays inside the invariant 'no other request is ever sent twice', but the text does not describe it, and the decision listed OPTIONS, which 6.8 forbids.

## Decision

node-agent-protocol 6.8 describes how the edge cancels requests to a backend that leaves rotation (GET/HEAD retried once, others 502, streaming bodies cut); OPTIONS stays out of the retry set.

A standalone contract PR amends 6.8 (Balancing, Timeouts): (1) when a backend leaves rotation the edge drops its pooled connections and cancels requests in flight to it; a GET/HEAD without response headers and with a resendable body is retried once on another backend in rotation; other requests answer 502 at once; a response already streaming is cut, never retried. (2) A GET/HEAD reset before response headers is retried once like a connect failure. (3) Backend sockets use TCP keepalive and TCP_USER_TIMEOUT. The retry set stays GET and HEAD, following the maintainer's decision of 2026-11-05: OPTIONS is rare behind an app edge, and CORS preflights are cheap to repeat client-side.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0166 in the project decision ledger. Do not edit by hand; changes go through a new decision.
