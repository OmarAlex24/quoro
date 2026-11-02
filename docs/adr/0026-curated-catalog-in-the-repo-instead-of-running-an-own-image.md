# ADR 0026: Curated catalog in the repo instead of running an own image registry (for now)

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0026
- **Tags:** catalog, security, oss
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Running a registry makes us responsible for the supply chain and costs infra. A catalog is also a community contribution engine (like Coolify one-click services).

## Decision

Curated catalog in the repo instead of running an own image registry (for now).

Templates point to pinned upstream images with verifiable signatures.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0026 in the project decision ledger. Do not edit by hand; changes go through a new decision.
