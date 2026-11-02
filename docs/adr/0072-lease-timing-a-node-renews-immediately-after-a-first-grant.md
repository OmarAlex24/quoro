# ADR 0072: Lease timing: a node renews immediately after a first grant and treats the lease as held only after that answer; held expiring leases survive an agent restart within the same boot by persisting remaining validity against boot_id and CLOCK_BOOTTIME

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0072
- **Tags:** reliability, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendations that closed these open questions on 2026-11-02. Removes the receipt-time assumption and keeps warm apps running across agent upgrades (fence only when boot_id changed).

## Decision

Lease timing: a node renews immediately after a first grant and treats the lease as held only after that answer; held expiring leases survive an agent restart within the same boot by persisting remaining validity against boot_id and CLOCK_BOOTTIME.

Late renewal = one unanswered renewal interval D/3, and session loss moves held expiring leases to uncertain, as M0 implements. Contract update in the pre-freeze contract PR; implementation before M5b.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0072 in the project decision ledger. Do not edit by hand; changes go through a new decision.
