# ADR 0024: Ingress: reverse proxy with ACME for public-IP hosts, Cloudflare Tunnel for homelabs behind NAT

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0024
- **Tags:** ingress
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The two target users need different ingress; Tunnel avoids opening ports and TLS fights.

## Decision

Ingress: reverse proxy with ACME for public-IP hosts, Cloudflare Tunnel for homelabs behind NAT.

Proxy implementation open (Caddy via admin API is the proven option; embedded Rust proxy is the alternative). Multiple cloudflared replicas on the same tunnel give basic failover; Cloudflare paid LB adds real health checks (pricing/behavior to verify). Both behind a common interface.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0024 in the project decision ledger. Do not edit by hand; changes go through a new decision.
