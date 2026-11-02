# ADR 0089: Privilege split in M2: the API and panel run unprivileged and only the node agent keeps the privileges the engine needs

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0089
- **Tags:** security, installer, engine
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted root for M1 (ADR 0088) on 2026-11-02; the root service is an M1-only trade-off.

## Decision

Privilege split in M2: the API and panel run unprivileged and only the node agent keeps the privileges the engine needs.

Lands when the node agent splits from the control plane in M2; the M2 plan must include it and a test that the API process holds no capabilities.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0089 in the project decision ledger. Do not edit by hand; changes go through a new decision.
