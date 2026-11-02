# ADR 0068: PRD HA table costs confirmed: none = no reserved resources; cold = backup storage and restore time

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0068
- **Tags:** product, reliability
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendations that closed these open questions on 2026-11-02. They follow directly from the policy definitions in D-0020.

## Decision

PRD HA table costs confirmed: none = no reserved resources; cold = backup storage and restore time.

Keep the PRD cells; add restore time to cold.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0068 in the project decision ledger. Do not edit by hand; changes go through a new decision.
