# ADR 0214: A planned switchover freezes the app with a VM pause, ships a final snapshot, and is forward-only after the revoke

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0214
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0177 promises a switchover without data loss; the freeze mechanism and failure handling are open.

## Decision

A planned switchover freezes the app with a VM pause, ships a final snapshot, and is forward-only after the revoke.

Steps: check in_sync, send command switchover_freeze (the source pauses the VM through the engine, ships a final snapshot, answers with its snapshot_seq and keeps the VM paused), revoke the source's lease, grant the target with a higher token, swap roles. Before the revoke, any failure sends switchover_thaw and emits app.switchover_failed (an event beyond the milestone plan's list, so a failed switchover is visible); after the revoke it only goes forward. A frozen source reports frozen: true; a restarted primary thaws it; a frozen source cut off fences when its lease ends. New engine methods pause and resume; a paused VM still reads Running.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0214 in the project decision ledger. Do not edit by hand; changes go through a new decision.
