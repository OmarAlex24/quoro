# ADR 0008: One microVM per app; no Docker on nodes

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0008
- **Tags:** engine, architecture
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

VM-level isolation matters when agents execute actions on real infra; removes Docker from nodes. Similar general architecture to Fly.io (reference only, not copying the product).

## Decision

One microVM per app; no Docker on nodes.

Deploy converts an OCI image into a root disk and boots it in a microVM with a minimal init. Fast microVM boot makes the 'warm' HA policy nearly as good as 'hot' (boot time to verify).

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0008 in the project decision ledger. Do not edit by hand; changes go through a new decision.
