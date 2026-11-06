# ADR 0230: A primary that learns of a higher term demotes itself with a new fatal session error not_primary and 503 on every request; quoro node rejoin moves its data aside

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0230
- **Tags:** m5b, contracts, promotion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0182 says an old primary that sees a higher term stops writing (503 not_primary) and waits, and that quoro node rejoin quarantines its divergent tail. How it tells its nodes, survives a restart, and what rejoin touches are open.

## Decision

A primary that learns of a higher term demotes itself with a new fatal session error not_primary and 503 on every request; quoro node rejoin moves its data aside.

On receiving stale_term (from a node or a voter) the primary stops expiring grants and renewals, records demoted_by_term in its local meta so a restart stays demoted, ends every node session with error not_primary (fatal; the node keeps redialing through its stored primary_urls, like after stale_term), and answers every session upgrade and every API, panel and MCP request with 503 not_primary. quoro node rejoin, as root, stops and disables the control-plane unit and moves the whole control-plane data directory (database and key files) aside as a quarantined copy named with the term and time, never deleting it. The host's agent reaches the new primary through its URLs and is told log_diverged, so it quarantines its followed copy and refollows from seq 1. Terminal and console sessions open on the old primary get no session.end record. Rejected: reusing stale_term for the old primary's own sessions, whose meaning (the other side's term is lower) does not fit.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0230 in the project decision ledger. Do not edit by hand; changes go through a new decision.
