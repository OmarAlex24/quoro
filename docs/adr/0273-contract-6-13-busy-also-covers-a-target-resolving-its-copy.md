# ADR 0273: Contract 6.13: busy also covers a target resolving its copy

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0273
- **Tags:** contracts, m5b, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The node already refuses busy while it resolves a failed swap's copy (#519), and the #499 fix adds a stream that finds the copy pending and the start-up resolution. Section 6.13's check list names only 'no other stream of this volume is open', so the contract no longer says why a target answers busy, and what snapshot_seq means in that answer.

## Decision

Contract 6.13: busy also covers a target resolving its copy.

Standalone contract PR, docs/contracts/node-agent-protocol.md section 6.13 only. (1) In the list of checks, replace 'no other stream of this volume is open.' with 'no other stream of this volume is open, and the target is not resolving its copy (a copy left pending by a crash or a failed swap, whose hash reads the whole volume; section 6.13 Applying).' (2) In Answer, add after the root_mismatch sentence: 'On busy the source keeps its base and drops only the snapshot it was shipping; the answer's snapshot_seq names the copy the target's store names as complete, which a copy being resolved may replace, so the source does not act on it.' No wire or schema change; the node already behaves this way. The sender's handling of busy (keep the base, drop next) is unchanged.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0273 in the project decision ledger. Do not edit by hand; changes go through a new decision.
