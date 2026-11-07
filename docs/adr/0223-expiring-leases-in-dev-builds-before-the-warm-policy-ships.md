# ADR 0223: Expiring leases in dev builds before the warm policy ships: a dev-only cargo feature lets ha_policy warm be accepted so the lease switches to expiring, with no replication

- **Status:** Accepted; refined by [ADR 0277](0277-warm-turns-on-at-the-end-of-its-chain-until-then-updateapp.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0223
- **Tags:** m5b, leases, lab
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0277](0277-warm-turns-on-at-the-end-of-its-chain-until-then-updateapp.md)

## Context

The lease timer, renewals and self-fence need fault tests on the test lab (cut, restart, expiry) before the warm policy ships; waiting would find timing bugs late.

## Decision

Expiring leases in dev builds before the warm policy ships: a dev-only cargo feature lets ha_policy warm be accepted so the lease switches to expiring, with no replication.

Conditions: the feature is off by default; the release build never enables it and CI asserts that; it unlocks only accepting ha_policy warm so the section 8.4 mode change to expiring fires, with no warm copy or replication; packaging/build-in-container.sh can enable it through an environment variable for test-lab dev builds only; it is removed when the warm policy merges. No contract change.

_Some wording was changed for the public repository._

## Consequences

- [ADR 0277](0277-warm-turns-on-at-the-end-of-its-chain-until-then-updateapp.md) refines when the dev-warm-policy feature is removed: with the last PR of the warm chain, which turns warm on for every build. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0223 in the project decision ledger. Do not edit by hand; changes go through a new decision.
