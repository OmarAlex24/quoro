# ADR 0272: Editorial: node-agent-protocol 5.11 and 6.7 say a pause_copy backup pauses the VM that attaches the volume, not the job's vm_id

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0272
- **Tags:** m5b, contracts, docs, backups, engine
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

With #548 (closes #524) the node marks the volume of a pause_copy backup as paused, not the job's vm_id, and every VM checks the marks on the volumes it attaches, because the engine pauses whichever VM attaches the volume (engine-trait 3.16), which differs from the job's vm_id after a redeploy, with a stale VM row on the primary, or when the job names no VM. ADR 0259 does not tie the mark to vm_id, so no decision changes. Two contract sentences still say the node pauses vm_id. Wording only.

## Decision

Editorial: node-agent-protocol 5.11 and 6.7 say a pause_copy backup pauses the VM that attaches the volume, not the job's vm_id.

Editorial: no behavior change and no code. Two sentences of docs/contracts/node-agent-protocol.md.

1. Section 5.11, spec.backups[], the vm_id row. Today: 'The app's VM on this node, which a pause_copy snapshot pauses; absent when the app has no VM here'. New: 'The app's VM on this node as the primary placed it; absent when the app has no VM here. The node does not use it to choose what to pause: a pause_copy snapshot pauses the VM that attaches the volume (engine-trait.md section 3.16), which a redeploy or a stale VM row can make another one.'

2. Section 6.7, the Backup bullet. Today: 'It then takes the snapshot with its method (engine-trait.md section 3.16), pausing vm_id for pause_copy, reports running, ...'. New: 'It then takes the snapshot with its method (engine-trait.md section 3.16), which for pause_copy pauses the VM that attaches the volume, reports running, ...'.

What vm_id still means: after #548 the node reads it for nothing in a backup; it stays an optional, informational field naming the VM the primary placed for the app on that node. Removing it would be a contract change on its own (an optional field the primary sends and older nodes read), not editorial, so this decision keeps it. Section 6.3's ADR 0259 bullet ('A VM that a backup has paused ...') names no vm_id and stays as written.

Wording: 'attaches', not 'mounts', to match engine-trait 3.16 ('If a VM that attaches the volume is Running'), the text ADR 0271 adds and #548's comments.

With ADR 0271: both are editorial, both describe the pause_copy path, and they touch different files with no overlapping text (ADR 0271: engine-trait 3.16 and 3.11; this: node-agent-protocol 5.11 and 6.7). ADR 0271's new 3.16 sentence ('A VM that attaches the volume and is already paused ... is Conflict') uses the same wording. Both land in one contract change.

Outside the contract: the doc comment of SpecBackup.vm_id in crates/proto/src/wire.rs ('which a pause_copy snapshot pauses') says the same old thing; that is code for the PR that fixes #524 (#548), not for the contract PR.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0272 in the project decision ledger. Do not edit by hand; changes go through a new decision.
