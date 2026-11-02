# ADR 0059: Q-0001: embedded Rust reverse proxy behind the Ingress interface; Caddy admin API as fallback backend

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0059
- **Tags:** ingress
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0012 one binary, D-0028 minimal dependencies, D-0007 rejected a Go daemon black box; the proxy needs health-aware routing and slow-start for D-0020/D-0022.

## Decision

Q-0001: embedded Rust reverse proxy behind the Ingress interface; Caddy admin API as fallback backend.

Options: (a) Caddy driven via its admin API: proven ACME (HTTP-01/TLS-ALPN-01, renewals), on-demand TLS; but a second Go process to ship, sign and update (D-0027 covers only our binary), and routing logic split across two processes. (b) Embedded Rust proxy (hyper + rustls + an ACME client crate): one binary, full control of health/slow-start/drain; more code we own, ACME edge cases are ours. Crate maturity was not researched (hypothesis) -> research task before M2. Accepted option: (b), behind the same Ingress trait that the Cloudflare Tunnel backend implements, keeping (a) as an alternative backend if the M2 spike shows ACME is too costly.

_Some wording was changed for the public repository._ _Some operational details were redacted for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0059 in the project decision ledger. Do not edit by hand; changes go through a new decision.
