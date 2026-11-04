# ADR 0127: Console: Engine::console superseded by serial_output; node ring of two 512 KiB segments; console streams outlive VM restarts

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0127
- **Tags:** contracts, m4
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

A second client on the CH serial socket silently takes it over

## Decision

Console: Engine::console superseded by serial_output; node ring of two 512 KiB segments; console streams outlive VM restarts.

console answers Conflict for socket-mode serial. Agent console sessions end when the 5-minute access token expires.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0127 in the project decision ledger. Do not edit by hand; changes go through a new decision.
