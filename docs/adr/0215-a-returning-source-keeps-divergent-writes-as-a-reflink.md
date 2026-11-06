# ADR 0215: A returning source keeps divergent writes as a reflink quarantine with a qrn_ id, deleted only by a human

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0215
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0176 keeps the returning source's divergent data in quarantine; how it is detected, recorded and removed is open.

## Decision

A returning source keeps divergent writes as a reflink quarantine with a qrn_ id, deleted only by a human.

The returning source compares its live volume file with its kept base and in-flight snapshot (volume_changed_ranges). No change against one of them: no divergent write, the file is deleted. Otherwise the live file becomes a quarantined copy (reflink clone), with a node-generated qrn_ id and SHA-256, reported in status_report.quarantines; the primary records it in volume_quarantines, writes the system audit record volume.quarantine and emits volume.quarantined. Volume.quarantines lists them. Only DELETE /api/v1/volumes/{volume_id}/quarantines/{quarantine_id} removes one (new scope app:delete_quarantine, destructive and human-only), reaching the node as spec.quarantine_deletes. deleteVolume and deleteApp are refused with has_quarantines while one exists, so an approved agent never deletes divergent writes nobody looked at.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0215 in the project decision ledger. Do not edit by hand; changes go through a new decision.
