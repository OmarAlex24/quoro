# ADR 0016: Leader election roadmap: single node in MVP, manual promotion in M5, Raft auto-failover with >=3 votes later

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0016
- **Tags:** reliability, control-plane
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Automatic election with only 2 nodes causes split-brain (both become primary after a network cut).

## Decision

Leader election roadmap: single node in MVP, manual promotion in M5, Raft auto-failover with >=3 votes later.

Manual promotion = command/button, or an agent with human approval. Automatic failover only with a majority (Raft).

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0016 in the project decision ledger. Do not edit by hand; changes go through a new decision.
