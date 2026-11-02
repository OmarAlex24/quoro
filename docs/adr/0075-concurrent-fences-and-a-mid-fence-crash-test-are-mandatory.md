# ADR 0075: Concurrent fences and a mid-fence crash test are mandatory before expiring leases ship (M5b)

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0075
- **Tags:** reliability
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendations that closed these open questions on 2026-11-02. The sequential M0 fencer can break the 8.4 bound once several expiring leases exist.

## Decision

Concurrent fences and a mid-fence crash test are mandatory before expiring leases ship (M5b).

Tracked as an M5b prerequisite issue.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0075 in the project decision ledger. Do not edit by hand; changes go through a new decision.
