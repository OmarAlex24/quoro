# ADR 0146: Volume snapshots for M5a: reflink (FICLONE) where the node's data dir supports it, otherwise pause the VM (CH vm.pause), sparse-copy the volume file, resume; the node reports which method it uses

- **Status:** Superseded by [ADR 0156](0156-volume-snapshots-reflink-where-the-node-s-data-dir-supports.md)
- **Date:** 2026-11-04
- **Ledger id:** D-0146
- **Tags:** m5a, engine, sync
- **Supersedes:** [ADR 0062](0062-q-0004-reflink-snapshots-full-chunk-scan-for-m5a-cold.md)
- **Superseded by:** [ADR 0156](0156-volume-snapshots-reflink-where-the-node-s-data-dir-supports.md)
- **Related:** none

## Context

D-0062 chose reflink snapshots, but both lab VMs (and Debian's default) are ext4 with no reflink (F-0241); without a fallback the cold-backup DoD cannot run on the lab.

## Decision

Volume snapshots for M5a: reflink (FICLONE) where the node's data dir supports it, otherwise pause the VM (CH vm.pause), sparse-copy the volume file, resume; the node reports which method it uses.

Engine: snapshot_volume probes FICLONE once per node; capabilities report snapshot: reflink | pause_copy; pause length is shown with the backup and in the HA cost; XFS for /var/lib/quoro-node documented as recommended. Rejected for M5a: dm-thin or a loop-mounted XFS pool (more moving parts; revisit with the lvm-thin vs btrfs spike before M5b). First lab-tested task of the volumes epic, measuring the pause on an ext4 test node. Recommendation: accept; keeps reflink as the primary path of D-0062.

_Some wording was changed for the public repository._

## Consequences

- Replaces ADR 0062.
- Superseded by [ADR 0156](0156-volume-snapshots-reflink-where-the-node-s-data-dir-supports.md); this decision is no longer binding and is kept for history.

---
Generated from decision D-0146 in the project decision ledger. Do not edit by hand; changes go through a new decision.
