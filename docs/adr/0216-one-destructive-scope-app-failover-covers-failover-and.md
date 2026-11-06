# ADR 0216: One destructive scope app:failover covers failover and switchover; it is not human-only

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0216
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The milestone plan names app:failover as destructive with JIT; whether the switchover shares it is open.

## Decision

One destructive scope app:failover covers failover and switchover; it is not human-only.

Both operations move the app's lease and its data to another node, so one scope keeps the approval flow simple. Agents need a JIT approval on {type: app} with node_id in params, as for restores. Operators and owners hold it.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0216 in the project decision ledger. Do not edit by hand; changes go through a new decision.
