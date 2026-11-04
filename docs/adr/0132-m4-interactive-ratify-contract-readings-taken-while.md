# ADR 0132: M4 interactive: ratify contract readings taken while implementing terminal and console sessions

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0132
- **Tags:** terminal, console, m4, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The M4 contract leaves these cases open; the code follows the readings below and a later change should be a deliberate contract PR, not drift.

## Decision

M4 interactive: ratify contract readings taken while implementing terminal and console sessions.

1) A max_uses grant consumed by the console open itself closes the session at the next re-check (5 s) with access_revoked, per permission-model 7.5 read literally (exhausted grant closes). Alternative: exempt the authorizing grant's use by the open. 2) session.end repeats the opener's accountability columns; when an owner ends someone else's session only ended_by names the owner. 3) A spent ticket answers 409 only while the session already has its WebSocket; a ticket burnt by a failed Origin check or presented for another session answers 401. 4) A transcript fetch for an unknown or console session is 404 and not audited (no vm target to name); an agent refusal names the session's VM when the session exists. 5) A terminal that ends (completed/vm_stopped) before its first data answers 409 at the open. 6) An agent's DELETE of its own session requires Quoro-Justification (validation_failed otherwise) and the justification is written on session.end. 7) Engine: SerialMode::Off keeps the legacy serial.log so pre-M4 specs keep boot logs; serial_output on a stopped VM replays the ring. 8) stream_input eof shuts the engine io, which the CH relay turns into CLOSE (SIGHUP), not a ^D; the control plane never sends eof today.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0132 in the project decision ledger. Do not edit by hand; changes go through a new decision.
