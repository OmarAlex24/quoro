# ADR 0172: Warm snapshot primitive: keep the reflink snapshots of D-0156 (native or the XFS volume pool) and find changed ranges by comparing the extent maps (FIEMAP) of two consecutive snapshots; no lvm-thin, no btrfs

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0172
- **Tags:** m5b, engine, sync
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0062 asked for an lvm-thin vs btrfs spike before M5b because cold backups lacked changed-block information. D-0156 since put every lab node on reflink (natively or through the loop-mounted XFS pool), so the volumes already sit on a filesystem that clones files cheaply. The open question is no longer which primitive but how to find what changed between two reflink snapshots.

## Decision

Warm snapshot primitive: keep the reflink snapshots of D-0156 (native or the XFS volume pool) and find changed ranges by comparing the extent maps (FIEMAP) of two consecutive snapshots; no lvm-thin, no btrfs.

Each interval the source node clones the volume (FICLONE, as cold backups do) and compares the new clone's FIEMAP extent list with the previous clone's: extents that still share physical blocks are unchanged; everything else (new, moved or unshared extents) is a changed range. A full hashed scan of the snapshot is the fallback when FIEMAP reports nothing useful or after a restart lost the previous clone. The previous clone is kept as the base until the receiver confirms the next one. pause_copy nodes (no pool possible) cannot hold warm apps. Engine-trait gains volume_changed_ranges(base, next) next to snapshot_volume (3.16), and the text that still names the lvm-thin vs btrfs spike (roadmap M5b, contracts README, engine-trait 3.16) is replaced. A lab spike first measures precision and correctness on an XFS volume pool. Refs, does not supersede, D-0156; D-0062 is already superseded. The maintainer accepted this on 2026-11-06, subject to that spike.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0172 in the project decision ledger. Do not edit by hand; changes go through a new decision.
