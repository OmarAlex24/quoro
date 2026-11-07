# ADR 0279: A fence's graceful phase also resumes a VM that a PauseCopy snapshot paused before it asks the guest to power off

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0279
- **Tags:** m5b, engine, backup, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0270](0270-a-graceful-stop-of-a-vm-that-a-pausecopy-snapshot-paused.md)

## Context

The maintainer, 2026-11-07, on a premise of D-0270 that was found wrong while implementing it (#584). D-0270 said a fence's stop is forced and so not concerned. But section 8.5 step 1 first asks for a graceful stop bounded by T_grace and forces it only when the grace is over. So the graceful phase of a fence also takes D-0270's path.

## Decision

A fence's graceful phase also resumes a VM that a PauseCopy snapshot paused before it asks the guest to power off.

Refines D-0270; the rest of D-0270 stands. During a fence's graceful phase (node-agent-protocol.md section 8.5 step 1, bounded by T_grace), a graceful stop of a VM that a PauseCopy snapshot paused ends the snapshot's pause as D-0270 says: it removes the record, resumes the VM and presses the power button. The snapshot then drops its copy. The guest may run for up to T_grace, exactly like a VM that was not paused, which the fence's bound already allows. A forced stop, including the fence's forced phase after T_grace, never resumes a VM. A section 3.22 freeze is never resumed by any stop. Section 3.22 and ADR 0270 say this. Rejected: a stop variant that never resumes, used only by fences (a contract change for no gain within the fence's bound).

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0270](0270-a-graceful-stop-of-a-vm-that-a-pausecopy-snapshot-paused.md): which stops resume a VM a PauseCopy snapshot paused: a fence's graceful phase too; only forced stops never resume. The rest of [ADR 0270](0270-a-graceful-stop-of-a-vm-that-a-pausecopy-snapshot-paused.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0279 in the project decision ledger. Do not edit by hand; changes go through a new decision.
