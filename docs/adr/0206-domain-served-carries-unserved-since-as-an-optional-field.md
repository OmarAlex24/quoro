# ADR 0206: domain.served carries unserved_since as an optional field (corrects ADR 0191)

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0206
- **Tags:** m5b, contracts
- **Supersedes:** [ADR 0191](0191-domain-unserved-and-domain-served-carry-domain-id-app-id.md)
- **Superseded by:** none
- **Related:** none

## Context

The first M5b contract change, which the maintainer accepted and merged in PR #362, has unserved_since optional, because no column records when a domain became unserved; ADR 0191 still said required.

## Decision

domain.served carries unserved_since as an optional field (corrects ADR 0191).

This decision restates ADR 0191 with one change: domain.served's unserved_since is optional (present when the primary knows it). Everything else in ADR 0191 holds through this decision. Contract text: PR #362.

_Some wording was changed for the public repository._

## Consequences

- Replaces ADR 0191.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0206 in the project decision ledger. Do not edit by hand; changes go through a new decision.
