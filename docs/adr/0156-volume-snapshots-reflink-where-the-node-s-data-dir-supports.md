# ADR 0156: Volume snapshots: reflink where the node's data dir supports it; otherwise volumes live in a node-managed loop-mounted XFS pool (reflink=1) so snapshots stay reflinks; pause_copy only as a last resort

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0156
- **Tags:** m5a, storage, engine
- **Supersedes:** [ADR 0146](0146-volume-snapshots-for-m5a-reflink-ficlone-where-the-node-s.md)
- **Superseded by:** none
- **Related:** none

## Context

Measured pause_copy on an ext4 test node: about 6-10 s for a nearly empty 1 GiB volume and 62-98 s for a full 5 GiB volume, so a cold backup would freeze the app for over a minute. The maintainer chose the loop-mounted XFS pool (2026-11-04).

## Decision

Volume snapshots: reflink where the node's data dir supports it; otherwise volumes live in a node-managed loop-mounted XFS pool (reflink=1) so snapshots stay reflinks; pause_copy only as a last resort.

Engine: snapshot_volume probes FICLONE on the volume directory once per node. If the data dir has no reflink, the node creates and mounts a sparse XFS image (mkfs.xfs -m reflink=1) under its data dir through a loop device, keeps volume files inside it and grows it as volumes are created; the host still never mounts a guest volume's filesystem. Capabilities report snapshot: reflink | pause_copy, where reflink covers both the native and the pool path. pause_copy remains only for nodes where the pool cannot be created (no loop device or no xfsprogs), and its pause length is shown with the backup and in the HA cost. XFS for the node data dir stays the documented recommendation. Supersedes ADR 0146 only in rejecting the loop-mounted XFS pool; dm-thin stays rejected for M5a.

_Some wording was changed for the public repository._

## Consequences

- Replaces ADR 0146.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0156 in the project decision ledger. Do not edit by hand; changes go through a new decision.
