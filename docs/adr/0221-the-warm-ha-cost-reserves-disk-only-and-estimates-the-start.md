# ADR 0221: The warm HA cost reserves disk only and estimates the start from the app's last observed start

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0221
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The milestone plan asks for a warm HaPolicyCost; its fields' meaning for warm is open.

## Decision

The warm HA cost reserves disk only and estimates the start from the app's last observed start.

reserved is the volume size plus the root disk on the warm node (memory and vCPUs are only needed after a failover, which the N+1 indicator checks). storage_bytes is the warm copy's allocated bytes as last reported (or the volume's as an estimate). restore_estimate_s is the time the app's last start took, or 10 s before one was observed. unavailable_reason gains no_warm_node.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0221 in the project decision ledger. Do not edit by hand; changes go through a new decision.
