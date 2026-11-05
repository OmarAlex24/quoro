# ADR 0062: Q-0004: reflink snapshots + full chunk scan for M5a cold backups; spike lvm-thin vs btrfs before M5b warm

- **Status:** Superseded by [ADR 0146](0146-volume-snapshots-for-m5a-reflink-ficlone-where-the-node-s.md)
- **Date:** 2026-11-02
- **Ledger id:** D-0062
- **Tags:** sync, engine
- **Supersedes:** none
- **Superseded by:** [ADR 0146](0146-volume-snapshots-for-m5a-reflink-ficlone-where-the-node-s.md)
- **Related:** none

## Context

F-0035: Cloud Hypervisor snapshots hold RAM and device state, not disk contents, and expose no dirty bitmap; the chunk engine needs its own point-in-time disk primitive. Warm (RPO seconds, D-0020) needs changed-block information, cold backups do not.

## Decision

Q-0004: reflink snapshots + full chunk scan for M5a cold backups; spike lvm-thin vs btrfs before M5b warm.

Options: (a) own block dirty tracking: needs a host block layer we control (e.g. dm-era or our own vhost-user-blk backend), large; (b) lvm-thin snapshots + thin_delta; (c) ZFS send (out-of-tree module on Debian, hypothesis); (d) btrfs snapshots/send; (e) reflink file copy (XFS/btrfs) + content-defined chunking of the whole file: simple, reads the full volume per run. Accepted option: (e) for M5a cold backups (interval in minutes); before M5b run a spike comparing (b) and (d) for few-second intervals. Engine trait exposes snapshot_volume / diff hooks so the primitive is swappable. The spike chooses the primitive for M5b.

_Some wording was changed for the public repository._

## Consequences

- Superseded by [ADR 0146](0146-volume-snapshots-for-m5a-reflink-ficlone-where-the-node-s.md); this decision is no longer binding and is kept for history.

---
Generated from decision D-0062 in the project decision ledger. Do not edit by hand; changes go through a new decision.
