# ADR 0210: Snapshots are numbered per volume, and the source keeps its base and the snapshot in flight across restarts

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0210
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0172 diffs consecutive reflink snapshots; how they are named, kept and recovered after a crash is not decided.

## Decision

Snapshots are numbered per volume, and the source keeps its base and the snapshot in flight across restarts.

snapshot_seq increases per volume on the source; a stream names its base by base_seq. The source keeps the base (the newest snapshot the target confirmed) and the snapshot it ships, records both in its local store, and the start-up sweep keeps recorded snapshots and removes the rest. A failed cycle keeps the base, deletes the new snapshot and retries next interval. This relaxes engine-trait 3.16: from M5b a volume may have several snapshots at once.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0210 in the project decision ledger. Do not edit by hand; changes go through a new decision.
