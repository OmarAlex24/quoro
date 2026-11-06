# ADR 0217: The N+1 indicator is GET /api/v1/cluster/capacity?lost_node_id under cluster:read; its MCP tool comes with the implementation

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0217
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0178 asks for an N+1 indicator; its endpoint, scope and MCP exposure are open.

## Decision

The N+1 indicator is GET /api/v1/cluster/capacity?lost_node_id under cluster:read; its MCP tool comes with the implementation.

The primary simulates the loss of one node with its current allocation: per app an action keep, start, failover or restore with the node it would use, or a reason no_capacity, no_backup or no_warm_copy; fits is false only for no_capacity, and missing gives the cpus, memory_mb and disk_gb short. No reservation. The MCP tool (get_capacity) comes with the implementation, because the contract test requires the README MCP table to equal the tools the crate implements.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0217 in the project decision ledger. Do not edit by hand; changes go through a new decision.
