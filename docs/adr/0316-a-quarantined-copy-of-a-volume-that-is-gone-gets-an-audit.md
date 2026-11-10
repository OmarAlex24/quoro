# ADR 0316: A quarantined copy of a volume that is gone gets an audit record and an event, without a row

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0316
- **Tags:** m5b, warm, quarantine, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0176](0176-returning-source-after-a-failover-it-keeps-its-divergent.md)

## Context

Divergent writes are never dropped and are always recorded (ADR 0022, ADR 0176). A returning source can report a quarantined copy after its volume was deleted, and migration 0011's foreign key then forbids the volume_quarantines row. Without a rule the copy stays on the node's disk with no audit record and no event.

## Decision

A quarantined copy of a volume that is gone gets an audit record and an event, without a row.

The first time a node lists a hashed quarantined copy of a volume that is gone or deleting, the primary writes the volume.quarantine audit record and emits volume.quarantined once, deduplicated by quarantine_id. It writes no volume_quarantines row, and the schema does not change. The copy is not listed in Volume.quarantines and deleteVolumeQuarantine cannot reach it, like a removed node's copy (node-agent protocol section 10); the operator removes the file on the node.

Not adopted now: refusing deleteApp and deleteVolume while a failover's old source has not returned. It can be raised again once removeNode is served, since without removeNode it would block deletes for a node that never returns.

The maintainer approved this option, a record without a row, on 2026-11-10. The contract text (sections 9 and 10) changes in a standalone contract change.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0176](0176-returning-source-after-a-failover-it-keeps-its-divergent.md): what a quarantined copy of a volume that is already gone gets: its audit record and its event, without a row. The rest of [ADR 0176](0176-returning-source-after-a-failover-it-keeps-its-divergent.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0316 in the project decision ledger. Do not edit by hand; changes go through a new decision.
