# ADR 0128: Transcripts: terminal only, 16 MiB cap closes session (transcript_limit), 30 days and 1 GiB per node, fetch human-only with audit:read and always audited

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0128
- **Tags:** contracts, m4
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Transcripts hold every secret typed or printed in a session

## Decision

Transcripts: terminal only, 16 MiB cap closes session (transcript_limit), 30 days and 1 GiB per node, fetch human-only with audit:read and always audited.

Console sessions have no transcript. The local interactive_sessions table closes orphaned sessions at primary start-up with close_reason primary_restarted; the open record carries session_id in params.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0128 in the project decision ledger. Do not edit by hand; changes go through a new decision.
