# ADR 0271: Editorial: engine-trait sections 3.16 and 3.11 name the PauseCopy errors and the resume after a restart that #534 implements

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0271
- **Tags:** m5b, engine, contracts, docs
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The second review of #534 found two contract-visible behaviors the text does not name: Conflict for an already paused VM (allowed by section 4, absent from the section 3.16 list) and the engine's start-up resuming a VM a snapshot left paused (section 3.11 says list never starts, restarts or deletes anything). Wording only.

## Decision

Editorial: engine-trait sections 3.16 and 3.11 name the PauseCopy errors and the resume after a restart that #534 implements.

Editorial: no behavior changes and no code. The contract text should name what #534 (issue #523) implements, which section 4 already allows. Two points:

### 1. The `PauseCopy` errors in section 3.16

Section 3.16 lists `NotFound`, `Conflict` (a second snapshot in M5a), `ResourceExhausted` and `Hypervisor` (a resume that fails). Section 4 lets the engine return `Conflict` for concurrent calls on one VM, so these are already allowed. Add after "A resume that fails is `Hypervisor`, and the VM is left for the node agent's crash handling.":

> The pause and the resume each take the VM's operation lock (section 4), which is free during the copy. A VM that attaches the volume and is already paused, by `pause` (section 3.22) or by a snapshot of another volume, is `Conflict`, since that freeze could end during the copy. A forced stop that waits for the lock gets it at once: before the pause the snapshot is then `Conflict`, and at the resume it is `Hypervisor`. A VM whose VMM stopped or was replaced during the copy is not resumed, its copy is not kept, and the snapshot is `Hypervisor`.

`FakeEngine` does not model the `Conflict` for a paused VM. That is a test double, and changing it is code, so it is outside this decision.

### 2. The resume after a restart, under sections 3.11 and 3.16

Section 3.11 says "`list` never starts, restarts or deletes anything". The resume #534 adds happens at engine start-up, in the rediscovery that section 5 describes, not in `list`. It resumes a VM, which is none of the three. Two sentences make that explicit.

In section 3.16, after the text above:

> Before the pause, the engine records the paused VMM process in the VM's runtime directory, and removes the record after the resume. When the engine starts again after a restart during the copy, it resumes that process once it adopts it, unless a stop of the VM was requested, a fence's included, which then ends it. A record of a process that is gone or runs is removed; while the VMM does not report whether the VM is paused, the record stays.

In section 3.11, after "`list` never starts, restarts or deletes anything.":

> Neither does the engine's start-up rediscovery, which only resumes a VM a `PauseCopy` snapshot left paused (section 3.16).

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0271 in the project decision ledger. Do not edit by hand; changes go through a new decision.
