# ADR 0176: Returning source after a failover: it keeps its divergent volume as a quarantined reflink snapshot on its own disk, reported to the primary and recorded in the audit log, then resyncs from the last common snapshot as the new warm copy

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0176
- **Tags:** m5b, sync, reliability, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0022 steps 1-4: the returning node starts nothing stateful, its divergent writes are quarantined and recorded, never dropped, and it resyncs incrementally from the last common point.

## Decision

Returning source after a failover: it keeps its divergent volume as a quarantined reflink snapshot on its own disk, reported to the primary and recorded in the audit log, then resyncs from the last common snapshot as the new warm copy.

On hello the primary revokes the stale lease (section 9); the node clones its volume into a quarantine file (reflink, so it costs only the diverged blocks), records it in its local quarantine table and reports it; the primary writes a system audit record volume.quarantine {app_id, volume_id, node_id, quarantine_id, size, sha256} and emits volume.quarantined. Both sides keep the last snapshot the old source shipped, which is the common base; the new source (the old target) ships the ranges changed since that base and the old source becomes the warm copy. Volume gains quarantines[] (id, node, created_at, size); DELETE /volumes/{volume_id}/quarantines/{id} is destructive and human-only; nothing deletes a quarantine automatically. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0176 in the project decision ledger. Do not edit by hand; changes go through a new decision.
