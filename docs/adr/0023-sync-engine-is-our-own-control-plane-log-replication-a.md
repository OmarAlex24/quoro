# ADR 0023: Sync engine is our own: control-plane log replication + a content-addressed chunk engine for volumes

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0023
- **Tags:** sync, reliability, backups
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer wants everything own and auditable; one engine serves both replication and backups.

## Decision

Sync engine is our own: control-plane log replication + a content-addressed chunk engine for volumes.

Volumes: snapshot -> split into content-addressed chunks (dedup, encrypted) -> ship only new chunks. To another node = warm replication; to S3/R2 = cold backup. Start with incremental snapshots every few seconds (RPO = interval). Exception: databases use their native replication orchestrated by Quoro (Postgres streaming, Litestream for SQLite), offered as catalog templates; block-level copies of a running DB are only crash-consistent.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0023 in the project decision ledger. Do not edit by hand; changes go through a new decision.
