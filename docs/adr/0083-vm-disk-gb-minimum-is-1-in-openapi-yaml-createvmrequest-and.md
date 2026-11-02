# ADR 0083: VM disk_gb minimum is 1 in openapi.yaml (CreateVmRequest and Vm), matching db-schema CHECK (disk_gb >= 1)

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0083
- **Tags:** control-plane, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-02. The API allowed 0 and the store refused it; the control plane already answers 422 for 0.

## Decision

VM disk_gb minimum is 1 in openapi.yaml (CreateVmRequest and Vm), matching db-schema CHECK (disk_gb >= 1).

Standalone contract PR. 'No data disk' is not a feature today; if it becomes one it gets its own field and decision.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0083 in the project decision ledger. Do not edit by hand; changes go through a new decision.
