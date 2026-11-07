# ADR 0250: A failed engine start, an error or a Timeout, waits the reconciler's crash backoff before the next start; an explicit restart clears it

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0250
- **Tags:** m5b, node-agent, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

A start that always overran its bound (3 s under an expiring lease) would otherwise be started and killed every few seconds (#462). Node-agent-protocol section 6.3 documents the behavior, as approved on 2026-11-06.

## Decision

A failed engine start, an error or a Timeout, waits the reconciler's crash backoff before the next start; an explicit restart clears it.

Section 6.3: after a failed start (the engine answered an error or Timeout), the node waits the same backoff as after a crash (1 s, doubling to 5 min, counted from the end of the failed start) before it starts the VM again. A successful start clears the backoff; a new restart_generation clears it and starts at once; the first start of a VM never waits; a failed start does not count as a restart. Implemented in #493; section 6.3 states it.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0250 in the project decision ledger. Do not edit by hand; changes go through a new decision.
