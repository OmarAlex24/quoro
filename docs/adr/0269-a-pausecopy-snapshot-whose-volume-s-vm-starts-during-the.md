# ADR 0269: A PauseCopy snapshot whose volume's VM starts during the copy is Conflict, not a torn copy reported Ok

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0269
- **Tags:** m5b, engine, backup, volumes, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Section 3.16 copies a volume no running VM attaches without a pause, and nothing stops a start of that VM during the copy, so a backup can be torn and still reported good. Found by the first review of #534 (issue #523); the contract is silent, so the maintainer decides.

## Decision

A PauseCopy snapshot whose volume's VM starts during the copy is Conflict, not a torn copy reported Ok.

### Scenario

On a `PauseCopy` node (no reflink and no volume pool, ADR 0156), `snapshot_volume` pauses only a VM that runs when it looks. Section 3.16: "A volume no running VM attaches is copied without a pause." Since #534 the engine reads each attaching VM's state under that VM's lock, so a start or a stop under way is waited out. But the lock is free during the copy, and nothing stops a start of a VM that attaches the volume while the copy runs:

1. The app's VM is stopped: a health restart's stop (#511) waiting out its crash backoff, a crash restart, or a stop the operator follows with a start.
2. A backup snapshots the volume. No attaching VM runs, so the engine copies without a pause. A 5 GiB volume takes 62 to 98 s (ADR 0156's measurement).
3. The node agent starts the VM during the copy. The guest boots, replays its journal and writes to the volume while the copy reads it.
4. The snapshot returns `Ok` with `pause` zero. The backup stores a copy whose blocks come from different moments. It is not a power-cut image, a journal replay may not repair it, and the backup is reported good.

The health-restart path makes this likely rather than rare: its backoff starts at 1 s, so a backup that begins while the VM is stopped almost always overlaps the start.

### The change

The engine detects a start during a copy without a pause, and refuses the snapshot:
- Before the copy, under each attaching VM's lock, the engine notes a start counter that every `start` increments under the lock. A counter catches a start and a stop that both fall inside the copy, which a check of the VMM process alone would miss.
- After the copy, it takes each attaching VM's lock again and compares the counter. If a VM was started meanwhile, the snapshot is `Conflict`, its file is removed and nothing is returned. A start under way at that moment is waited out, within the start's own bound.
- The node agent treats that `Conflict` like any failed backup snapshot and tries again at the next schedule.

### Sentence for section 3.16

Replace "A volume no running VM attaches is copied without a pause." with:

> A volume no running VM attaches is copied without a pause. If a VM that attaches it starts during that copy, the snapshot is `Conflict` and leaves no file, since the guest may have written to the volume while it was copied.

### Alternatives

- **Hold the VM's lock across the copy.** A start would wait for up to the whole copy, a minute or more, and break the start's bound.
- **The node agent does not start a VM whose volume a backup copies.** It would extend the backup's pause mark (#511) to copies without a pause. This keeps the engine contract as it is, but a start waits for up to the whole copy, and `node-agent-protocol.md` needs the rule. It could come later, on top of the engine check, to avoid failed backups.
- **Leave it.** A backup can be torn and still reported good.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0269 in the project decision ledger. Do not edit by hand; changes go through a new decision.
