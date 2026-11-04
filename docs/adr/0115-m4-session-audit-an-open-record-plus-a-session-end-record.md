# ADR 0115: M4 session audit: an open record plus a session-end record carrying a SHA-256 of a node-local, size-capped transcript

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0115
- **Tags:** m4, terminal, audit, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

One audit record per call does not describe a minutes-long session; parsing commands from a PTY is unreliable; shipping transcripts to the primary contradicts D-0014 and puts secrets in the replicated, exported audit log. A hash in the chained close record makes transcript tampering evident (D-0031).

## Decision

M4 session audit: an open record plus a session-end record carrying a SHA-256 of a node-local, size-capped transcript.

Open: the normal record for the POST (target vm/app, params rows, cols, command, user). New recorded action session.end written by the primary on any close, same actor/on_behalf_of/channel, params {session_id, kind terminal|console, duration_ms, bytes_in, bytes_out, exit_code?, close_reason, transcript_sha256?, transcript_bytes?}. Transcript = timed input+output (asciicast-style) on the node, capped; access needs audit:read; retention and fetch path to decide in the contract PR. Transcripts contain secrets by design. Evidence: docs/research/m4-terminal-and-console.md (PR #155, issue #151).

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0115 in the project decision ledger. Do not edit by hand; changes go through a new decision.
