# ADR 0186: Add #287 (old node recreates a moved app's VM) and #240 (terminal output lost, upgrade answers 401) to the M5b milestone and fix them before the failover work

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0186
- **Tags:** m5b, roadmap
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Failover and switchover move an app's VM between nodes through the same path as a restore, which #287 shows leaves a stopped VM on the old node and sends terminal sessions to it (#240's 401). Issues and milestones are the task source of truth (D-0036), so they must be in the milestone to be worked.

## Decision

Add #287 (old node recreates a moved app's VM) and #240 (terminal output lost, upgrade answers 401) to the M5b milestone and fix them before the failover work.

#287: the primary drops the VM from the old node's spec.vms when the app moves, so the old node deletes it. #240: keep a finished session's output and exit frames until its ticket expires and replay them on attach, inside the current contract; if a distinct status code is wanted it goes into the first M5b contract change. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0186 in the project decision ledger. Do not edit by hand; changes go through a new decision.
