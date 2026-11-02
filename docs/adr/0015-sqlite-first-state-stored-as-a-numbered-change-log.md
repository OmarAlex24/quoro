# ADR 0015: SQLite first; state stored as a numbered change log

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0015
- **Tags:** architecture, control-plane
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

'One binary, one command' conflicts with requiring Postgres; single leader in MVP; supporting two DBs with sqlx compile-time macros is painful.

## Decision

SQLite first; state stored as a numbered change log.

Bonus: Cloudflare D1 is SQLite-based. Backup of the control plane itself to S3/R2 (Litestream was suggested; to verify vs own chunk engine).

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0015 in the project decision ledger. Do not edit by hand; changes go through a new decision.
