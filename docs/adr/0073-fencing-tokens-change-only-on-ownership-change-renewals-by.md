# ADR 0073: Fencing tokens change only on ownership change: renewals by the same holder keep the token; a grant to a new holder or after expiry or revoke needs a strictly higher token

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0073
- **Tags:** reliability, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendations that closed these open questions on 2026-11-02. Standard fencing-token semantics; keeps the equality check in node-agent-protocol 6.4 valid across renewals.

## Decision

Fencing tokens change only on ownership change: renewals by the same holder keep the token; a grant to a new holder or after expiry or revoke needs a strictly higher token.

Update 5.5/5.12 in the pre-freeze contract PR and the M0 lease machine accordingly. A revoke with a lower token than the current one is refused with bad_message (as M0 does).

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0073 in the project decision ledger. Do not edit by hand; changes go through a new decision.
