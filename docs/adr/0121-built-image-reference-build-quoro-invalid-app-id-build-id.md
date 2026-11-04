# ADR 0121: Built image reference build.quoro.invalid/<app_id>:<build_id>; Git app image is empty before first build

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0121
- **Tags:** contracts, m4
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Reserved .invalid TLD can never be pulled; keeps apps.image NOT NULL

## Decision

Built image reference build.quoro.invalid/<app_id>:<build_id>; Git app image is empty before first build.

spec.vms[].build_id names the local root disk; nodes never pull build.quoro.invalid references.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0121 in the project decision ledger. Do not edit by hand; changes go through a new decision.
