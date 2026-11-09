# ADR 0291: Engine-trait section 6 names quoro-init's sync refusal 131 for an earlier syncfs that has not returned, and the rule when the host gives up after its 2 s

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0291
- **Tags:** m5b, engine, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer, 2026-11-09. The init side of ADR 0282 (#635) refuses a sync session with 131 while an earlier syncfs of the same volume has not returned, so a hung syncfs does not hold a session slot, but the contract does not name that refusal or the host's rule after its 2 s timeout.

## Decision

Engine-trait section 6 names quoro-init's sync refusal 131 for an earlier syncfs that has not returned, and the rule when the host gives up after its 2 s.

A standalone contract PR, written by the F lane as a path exception as with ADR 0282, adds two sentences to engine-trait section 6 (sync sessions): quoro-init answers 131 'sync: an earlier syncfs of quoro-vol-N has not returned' while an earlier syncfs of that volume is still running, and when the host gives up after its 2 s bound the snapshot proceeds crash-consistent with guest_flush false while the guest's syncfs may still finish later.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0291 in the project decision ledger. Do not edit by hand; changes go through a new decision.
