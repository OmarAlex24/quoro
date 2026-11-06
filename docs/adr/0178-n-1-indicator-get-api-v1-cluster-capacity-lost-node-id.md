# ADR 0178: N+1 indicator: GET /api/v1/cluster/capacity?lost_node_id= answers whether that node's apps fit on the others, or which resources are missing; shown in the panel's HA view

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0178
- **Tags:** m5b, control-plane, panel, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0021 and the DoD ask 'if node A dies, do its apps fit on the others?', answered yes or no plus what is missing. The HA cost fields already shipped in M5a (AppHaCost), so only N+1 remains of the roadmap's 'HA policy cost and N+1 fields' contract line.

## Decision

N+1 indicator: GET /api/v1/cluster/capacity?lost_node_id= answers whether that node's apps fit on the others, or which resources are missing; shown in the panel's HA view.

Scope cluster:read, so agents can watch it (MCP tool get_capacity under the same scope). The primary simulates the node's loss: stateless apps lose that replica (no move needed while another runs); warm apps start on their warm node; none/cold apps need a restore on any node with room. It places them greedily by memory then vCPU then disk against each remaining node's capacity minus allocation and answers {lost_node_id, fits, missing {cpus, memory_mb, disk_gb}, apps[{app_id, policy, action, node_id|null, reason}]}. No event in M5b. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0178 in the project decision ledger. Do not edit by hand; changes go through a new decision.
