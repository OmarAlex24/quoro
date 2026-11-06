# ADR 0219: The Engine trait adds volume_changed_ranges, staging calls, pause and resume, all Unsupported by default

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0219
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

engine-trait 3.16 deferred the changed-ranges call to the warm contract.

## Decision

The Engine trait adds volume_changed_ranges, staging calls, pause and resume, all Unsupported by default.

New section 3.22: volume_changed_ranges(newer snapshot or live file, optional base) compares FIEMAP extent maps (FIEMAP_FLAG_SYNC; unknown, delalloc, encoded, not-aligned, inline or tail extents count as changed) and falls back to every allocated range with full: true; it reports extent_count. stage_volume / commit_staged_volume / discard_staged_volume for the target's copy (clone, write, fsync, rename). pause / resume for the switchover. No existing type gains a field and VmState gains no variant.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0219 in the project decision ledger. Do not edit by hand; changes go through a new decision.
