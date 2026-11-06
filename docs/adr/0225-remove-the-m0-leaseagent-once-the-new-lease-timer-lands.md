# ADR 0225: Remove the M0 LeaseAgent once the new lease timer lands, after porting the tests that still apply to it

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0225
- **Tags:** m5b, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Only tests use it; the new renewal and expiry timer replaces its timer half.

## Decision

Remove the M0 LeaseAgent once the new lease timer lands, after porting the tests that still apply to it.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0225 in the project decision ledger. Do not edit by hand; changes go through a new decision.
