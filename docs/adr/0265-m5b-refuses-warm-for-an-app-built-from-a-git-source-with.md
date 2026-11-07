# ADR 0265: M5b refuses warm for an app built from a Git source, with the reason not_in_this_release

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0265
- **Tags:** m5b, warm, contracts, builds
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Decided on 2026-11-07. A built image exists only on the node that built it, so a warm node could not start the app after a failover.

## Decision

M5b refuses warm for an app built from a Git source, with the reason not_in_this_release.

ha_cost.warm reads available: false with reason not_in_this_release for an app with a source, and updateApp refuses warm for it. The contract PR adds one sentence to openapi.yaml's HaPolicyCost.reason text; no new enum value. Rejected for M5b: B (build again on the warm node) and C (send the built image over the mesh; candidate for M6).

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0265 in the project decision ledger. Do not edit by hand; changes go through a new decision.
