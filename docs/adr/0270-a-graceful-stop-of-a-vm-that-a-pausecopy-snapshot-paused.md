# ADR 0270: A graceful stop of a VM that a PauseCopy snapshot paused resumes it and asks the guest to power off, instead of a hard shutdown

- **Status:** Accepted; refined by [ADR 0279](0279-a-fence-s-graceful-phase-also-resumes-a-vm-that-a-pausecopy.md)
- **Date:** 2026-11-07
- **Ledger id:** D-0270
- **Tags:** m5b, engine, backup, volumes, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0279](0279-a-fence-s-graceful-phase-also-resumes-a-vm-that-a-pausecopy.md)

## Context

Today a stop or a redeploy during a PauseCopy backup powers the app off hard, because a graceful stop of a paused VM skips the power button (section 3.22). Changing it changes stop's semantics and must not touch a switchover freeze (ADR 0177), so it needs an ADR. Found by the first review of #534 (issue #523).

## Decision

A graceful stop of a VM that a PauseCopy snapshot paused resumes it and asks the guest to power off, instead of a hard shutdown.

### Scenario

Section 3.22 says "`stop` and `delete` work on a paused VM". A paused guest cannot answer the power button, so the engine's graceful stop of a paused VM goes straight to the VMM's shutdown, then the kill, instead of waiting out its graceful timeout. That is the right call for a §3.22 freeze. It also applies to a VM a `PauseCopy` snapshot paused, which stays paused for the whole copy, 62 to 98 s for a 5 GiB volume (ADR 0156):

1. A backup snapshots the volume of a running VM on a `PauseCopy` node, and the engine pauses the VM for the copy.
2. During the copy the operator stops the app, or redeploys it. A fence's stop is forced, so it is not concerned, and the health restart already skips a VM it knows is paused for a backup (#511, ADR 0259).
3. The graceful stop sees the VM `Paused` and shuts the VMM down. The guest gets no power button: the app never sees SIGTERM, unflushed data is lost, and the filesystem gets a power-cut recovery at the next start.
4. A second path: the paused probe has a 200 ms timeout. A VMM that answers it late reads as not paused, so the stop sends the power button to a paused guest, which cannot answer. The stop then holds the VM's lock for its whole graceful timeout before the forced stop.

The backup fails either way ("stopped while it was paused for the copy"), which is correct.

### The change

A graceful stop of a VM that a `PauseCopy` snapshot paused ends the snapshot's pause itself, then stops the guest as usual:
- **Who paused it.** The engine already records, in the VM's directory, the VMM process a snapshot paused (`snapshot-paused.json`, #534). The stop acts only when that record names the VM's live process. A §3.22 freeze has no record, so it is never resumed by a stop.
- **What the stop does.** Under its lock, the stop removes the record, resumes the VM, and sends the power button within its usual graceful bound. A guest that does not stop in time gets the forced stop, as now.
- **The snapshot's side.** The record becomes the snapshot's claim on its pause. At its resume, a snapshot whose record is gone or names another process does not resume the VM and drops its copy (`Hypervisor`), since the guest ran during the copy. That also covers a stop whose caller drops its future after the resume, while the guest still runs on the same process.
- **Forced stops** (fences) are unchanged: they need no resume and keep their bound (ADR 0243).

### Why this needs an ADR

- It changes `stop`'s semantics in section 3.22: a stop would resume a VM before asking it to power off, a state change the guest sees, where today a paused VM is never resumed by a stop.
- It must never resume a planned switchover's freeze (ADR 0177), so it depends on telling a snapshot's pause from a §3.22 pause. The record does that today, and the contract should say so.
- It trades two things the maintainer weighs: a clean app shutdown during a backup, against keeping that backup's pause intact (the backup fails in both cases).
- The stop's graceful budget now includes a resume call, inside the bound ADR 0243 sets.

### Alternatives

- **Leave it, and document it.** A stop during a `PauseCopy` backup is a hard power-off of the app. Backups of a 5 GiB volume pause for up to about 100 s, so a redeploy during a backup hits it.
- **The node agent waits for the backup before it stops the VM.** It would extend the backup's pause mark (#511) to stops: a stop or a redeploy waits for up to the whole copy, while fences still go through at once. This keeps the engine contract as it is, but needs a `node-agent-protocol.md` rule and delays operator actions.

_Some wording was changed for the public repository._

## Consequences

- [ADR 0279](0279-a-fence-s-graceful-phase-also-resumes-a-vm-that-a-pausecopy.md) refines which stops resume a VM a PauseCopy snapshot paused: a fence's graceful phase too; only forced stops never resume. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0270 in the project decision ledger. Do not edit by hand; changes go through a new decision.
