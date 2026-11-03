# ADR 0111: user:read stays human-only; WebhookDelivery exposes the test flag; creating users is M4 work

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0111
- **Tags:** contracts, m3
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-03.

## Decision

user:read stays human-only; WebhookDelivery exposes the test flag; creating users is M4 work.

(1) user:read is in every human role bundle and refused to agents (human_only), as contract PR #133 shipped; agents never read human emails. (2) Additive contract change: WebhookDelivery gains a boolean 'test' (false for real deliveries) backed by webhook_deliveries.test (migration 0004); the panel marks test deliveries. (3) A path to create and invite users (createUser and its panel screen) is planned in M4; until then a second user cannot be created and lab proofs needing one stay on integration tests.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0111 in the project decision ledger. Do not edit by hand; changes go through a new decision.
