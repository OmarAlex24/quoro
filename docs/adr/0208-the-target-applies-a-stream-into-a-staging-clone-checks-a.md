# ADR 0208: The target applies a stream into a staging clone, checks a root hash over 1 MiB cells, and renames it in one step

- **Status:** Accepted; refined by [ADR 0248](0248-warm-replication-a-root-mismatch-answer-makes-the-source-s.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0208
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0248](0248-warm-replication-a-root-mismatch-answer-makes-the-source-s.md)

## Context

ADR 0173 requires that the target never holds a half-applied copy but does not say how a copy is verified or committed.

## Decision

The target applies a stream into a staging clone, checks a root hash over 1 MiB cells, and renames it in one step.

A volume is cut into 1 MiB cells; a cell hash is BLAKE3 of its bytes and the root hash is BLAKE3 of the concatenated cell hashes. Each side keeps the cell hashes of its copies and updates only touched cells. The target writes into a staging file (a reflink clone of the copy base_seq names, or an empty sparse file for a full copy), checks every block hash, compares the root hash at the manifest, then syncs and renames over its copy. Refusal reasons: stale_token, wrong_lease, not_target, not_newer, base_mismatch, root_mismatch, bad_frame, busy. base_mismatch makes the source ship a full copy.

## Consequences

- [ADR 0248](0248-warm-replication-a-root-mismatch-answer-makes-the-source-s.md) refines what the source does with its base when the target refuses with root_mismatch. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0208 in the project decision ledger. Do not edit by hand; changes go through a new decision.
