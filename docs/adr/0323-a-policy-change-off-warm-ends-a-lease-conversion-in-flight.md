# ADR 0323: A policy change off warm ends a lease conversion in flight

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0323
- **Tags:** m5b, leases, warm, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0244](0244-a-converted-lease-s-row-keeps-mode-local-until-the-primary.md)

## Context

The maintainer answered this on 2026-11-10 as the answer to an open question, so the ledger held it as an answer and no ADR was generated. The rule narrows ADR 0244 and is already in the contract (section 8.4, 'Changing the mode'). The maintainer asked on 2026-11-10 that it be recorded as a decision so the ADRs carry it. This entry restates the answer and adds nothing.

## Decision

A policy change off warm ends a lease conversion in flight.

When an app's policy changes off warm while its lease is being converted from local to expiring, the conversion ends. The primary answers the holder's renewals as the contract says for the time from the policy change until the holder has taken the grant, and writes no expiring lease for them. The app keeps running, and what the API shows matches the policy. This refines ADR 0244 only for a conversion in flight; the rest of ADR 0244 stands.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0244](0244-a-converted-lease-s-row-keeps-mode-local-until-the-primary.md): what a policy change off warm does to a lease conversion in flight: the conversion ends, and no `expiring` lease is written for it. The rest of [ADR 0244](0244-a-converted-lease-s-row-keeps-mode-local-until-the-primary.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0323 in the project decision ledger. Do not edit by hand; changes go through a new decision.
