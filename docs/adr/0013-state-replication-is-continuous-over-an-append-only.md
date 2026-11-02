# ADR 0013: State replication is continuous over an append-only, numbered, hash-chained log

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0013
- **Tags:** architecture, control-plane, reliability
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Periodic sync would lose everything since the last sync when the primary dies, including the audit log.

## Decision

State replication is continuous over an append-only, numbered, hash-chained log.

Every state change is a numbered entry; replicas ask 'give me everything after N' and verify the hash chain (tamper-evident).

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0013 in the project decision ledger. Do not edit by hand; changes go through a new decision.
