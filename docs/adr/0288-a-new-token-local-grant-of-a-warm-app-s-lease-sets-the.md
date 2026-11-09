# ADR 0288: A new-token local grant of a warm app's lease sets the unsettled mark whatever the row reads, which also covers the conversion window

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0288
- **Tags:** m5b, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0285](0285-a-volume-restore-that-turns-an-expiring-lease-row-local-at.md)

## Context

The maintainer, 2026-11-09, ratified the wider reading of D-0285 written in its contract change. Without it, a volume restore during the conversion window of ADR 0244, while the row still reads local, would grant local on an unmarked row and leave the same double-run gap.

## Decision

A new-token local grant of a warm app's lease sets the unsettled mark whatever the row reads, which also covers the conversion window.

Refines D-0285. Any grant of a new token in mode local for an app whose policy is warm sets the unsettled mark as R does, whatever the row reads, so its grant waits until every machine with the node role holds the write and the primary holds majority support. The threat model's T-LEASE-01 row states this rule.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0285](0285-a-volume-restore-that-turns-an-expiring-lease-row-local-at.md): which writes set the mark: a new-token local grant of a warm app's lease sets it whatever the row reads, which covers the conversion window of ADR 0244. The rest of [ADR 0285](0285-a-volume-restore-that-turns-an-expiring-lease-row-local-at.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0288 in the project decision ledger. Do not edit by hand; changes go through a new decision.
