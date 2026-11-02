# ADR 0069: SQLite and catalog-template backups use Quoro's own chunk engine with client-side encryption, from consistent snapshots (SQLite backup API / VACUUM INTO)

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0069
- **Tags:** sync, security
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendations that closed these open questions on 2026-11-02. Keeps encryption keys away from providers and avoids the unmaintained Litestream 0.3 line; D-0023 already makes the chunk engine ours. Losing Litestream's ~1 s RPO is acceptable for the cold policy.

## Decision

SQLite and catalog-template backups use Quoro's own chunk engine with client-side encryption, from consistent snapshots (SQLite backup API / VACUUM INTO).

Litestream with SSE-C may be added later as an optional backend for users who want lower RPO. Control-plane state replication stays the change log (D-0013).

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0069 in the project decision ledger. Do not edit by hand; changes go through a new decision.
